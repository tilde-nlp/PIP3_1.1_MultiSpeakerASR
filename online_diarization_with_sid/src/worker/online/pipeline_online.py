import logging
from collections import OrderedDict, deque
import os
import importlib
import json

import gi

gi.require_version("Gst", "1.0")
from gi.repository import GObject, Gst

GObject.threads_init()
Gst.init(None)

from typing import Optional, Any, Dict, Callable, Tuple

logger = logging.getLogger(__name__)


class TranscriberOnlinePipeline(object):

    def __init__(self,
                 conf: Optional[Dict[str, Any]] = None) -> None:
        if conf is None:
            conf = {}
        logger.info("Creating decoder using conf: %s" % conf)

        # import diarization implementation
        diariz_class: str = conf.get("diarization", {}).get("class", None)

        if diariz_class:
            diariz_mod, diariz_class = diariz_class.rsplit(".", 1)
            self.diariz_impl = getattr(importlib.import_module(diariz_mod), diariz_class)
            self.diariz_conf = conf["diarization"]
            self.diariz = self.diariz_impl(self.diariz_conf)
            self.diariz.set_diariz_handler(self._on_diariz)
        else:
            self.diariz = None

        self.use_cutter = conf.get("use-vad", False)

        self._create_pipeline(conf)

        self.outdir: Optional[str] = conf.get("out-dir", None)
        if self.outdir and not os.path.isabs(self.outdir):
            self.outdir = "%s/%s" % (os.getcwd(), self.outdir)

        self.word_handler: Optional[Callable] = None
        self.result_handler: Optional[Callable] = None
        self.full_result_handler: Optional[Callable] = None
        self.eos_handler: Optional[Tuple] = None
        self.error_handler: Optional[Callable] = None
        self.request_id: str = "<undefined>"

    def _create_pipeline(self, conf: Dict[str, Any]) -> None:
        self.appsrc = Gst.ElementFactory.make("appsrc", "appsrc")
        self.decodebin = Gst.ElementFactory.make("decodebin", "decodebin")
        self.audioconvert = Gst.ElementFactory.make("audioconvert", "audioconvert")
        self.audioresample = Gst.ElementFactory.make("audioresample", "audioresample")
        self.capsfilter = Gst.ElementFactory.make("capsfilter", "capsfilter")
        self.tee = Gst.ElementFactory.make("tee", "tee")
        self.queue1 = Gst.ElementFactory.make("queue", "queue1")
        self.filesink = Gst.ElementFactory.make("filesink", "filesink")
        self.queue2 = Gst.ElementFactory.make("queue", "queue2")
        self.queue3 = Gst.ElementFactory.make("queue", "queue_appsink")
        self.cutter = Gst.ElementFactory.make("cutter", "cutter")
        self.asr = Gst.ElementFactory.make("kaldinnet2onlinedecoder", "asr")
        self.fakesink = Gst.ElementFactory.make("fakesink", "fakesink")
        self.asrsink = Gst.ElementFactory.make("appsink", "appsink")

        # validate and correct decoder config dictionary order...
        conf_dict_src = conf.get("decoder", {})
        conf_dict: OrderedDict = OrderedDict()
        # nnet-mode must be first
        if "nnet-mode" in conf_dict_src:
            conf_dict["nnet-mode"] = conf_dict_src.pop("nnet-mode")
        # copy rest of the stuff in order
        for k, v in conf_dict_src.items():
            conf_dict[k] = v
        # "fst" and "model" properties must be last,
        # see https://github.com/alumae/kaldi-gstreamer-server/pull/116
        if "fst" in conf_dict:
            conf_dict["fst"] = conf_dict.pop("fst")
        if "model" in conf_dict:
            conf_dict["model"] = conf_dict.pop("model")

        # set properties
        for (key, val) in conf_dict.items():
            logger.info("Setting decoder property: %s = %s"
                        % (key, val))
            self.asr.set_property(key, val)

        for (key, val) in conf.get("cutter", {}).items():
            logger.info("Setting cutter property: %s = %s"
                        % (key, val))
            self.cutter.set_property(key, val)

        if self.use_cutter:
            self.asr.set_property("silent", True)

        self.appsrc.set_property("is-live", True)
        self.filesink.set_property("location", "/dev/null")
        caps = Gst.caps_from_string("audio/x-raw, format=S16LE, channels=1, rate=16000")
        self.capsfilter.set_property("caps", caps)
        logger.info('Created GStreamer elements')

        self.pipeline = Gst.Pipeline()
        for element in [self.appsrc,
                        self.decodebin,
                        self.audioconvert,
                        self.audioresample,
                        self.tee,
                        self.queue1,
                        self.filesink,
                        self.cutter,
                        self.capsfilter,
                        self.queue2,
                        self.queue3,
                        self.asr,
                        self.asrsink,
                        self.fakesink]:
            logger.debug("Adding %s to the pipeline" % element)
            self.pipeline.add(element)

        logger.info('Linking GStreamer elements')

        self.audioconvert.link(self.audioresample)

        self.audioresample.link(self.capsfilter)

        if self.use_cutter:
            self.capsfilter.link(self.cutter)
            self.cutter.link(self.tee)
        else:
            self.capsfilter.link(self.tee)

        self.tee.link(self.queue1)
        self.queue1.link(self.filesink)

        self.tee.link(self.queue2)
        self.queue2.link(self.asr)
        self.asr.link(self.fakesink)

        self.tee.link(self.queue3)
        self.queue3.link(self.asrsink)
        self.asrsink.set_property("emit-signals", True)
        self.asrsink.set_property("sync", False)

        # Create bus and connect several handlers
        self.bus = self.pipeline.get_bus()
        self.bus.add_signal_watch()
        self.bus.enable_sync_message_emission()
        self.bus.connect('message::eos', self._on_eos)
        self.bus.connect('message::error', self._on_error)

        self.bus.connect('sync-message::element',
                         self._on_element_message)

        self.asrsink.connect('eos', self._on_appsink_eos)
        self.asrsink.connect('new-sample', self._on_new_sample)
        self.asrsink.connect('new-preroll', self._on_appsink_preroll)

        self.asr.connect('partial-result',
                         self._on_partial_result)
        self.asr.connect('final-result',
                         self._on_final_result)
        self.asr.connect('full-final-result',
                         self._on_full_final_result)

    def _connect_decoder(self, element, pad) -> None:
        logger.info("%s: Connecting audio decoder"
                    % self.request_id)
        pad.link(self.audioconvert.get_static_pad("sink"))
        logger.info("%s: Connected audio decoder"
                    % self.request_id)

    def _on_element_message(self, bus, message) -> None:
        if message.has_name("cutter"):
            if message.get_structure().get_value("above"):
                logger.debug("VAD: speech")
                self.asr.set_property("silent", False)
            else:
                logger.debug("VAD: silence")
                self.asr.set_property("silent", True)

    def _on_diariz(self, annotation):
        if annotation[2] == "done":
            # diarization done, output all buffered results
            self.diariz_done = True
            self._output_delayed_results()
            return

        if not self.current_segment:
            self.current_segment = annotation

        # if speaker changed
        if self.current_segment[2] != annotation[2]:
            # save speaker timestamp and speaker id
            self.speaker_change.append((annotation[0], self.current_segment[2]))
            logger.debug("Speaker change detected at %s: %s => %s" % (annotation[0], self.current_segment[2], annotation[2]))
            self.current_segment = annotation

        # output delayed results, i.e. final hyps waiting for diarization
        self._output_delayed_results()

    def _output_delayed_results(self):

        if self.diariz_done:
            # diarization done, output all buffered results
            for res in self.result_buffer:
                if self.diariz_conf.get("as_partial_vad", False):
                    # diarization marked these segments as non-speech
                    # clear recognition results
                    res["result"]["hypotheses"] = [{"transcript": "", "word-alignment": []}]
                res["speaker"] = self.current_segment[2] if self.current_segment else "speaker0"
                self.full_result_handler(res)
            self.result_buffer: deque[Any] = deque()
            self.diariz_done = True
            return

        # process all ASR final hyps before current segment
        while self.result_buffer and self.result_buffer[0]["segment-start"] < self.current_segment[0]:
            res = self.result_buffer.popleft()
            if not self.speaker_change:
                # no speaker change detected so far
                res["speaker"] = self.current_segment[2]
                self.full_result_handler(res)
            elif res["segment-start"] + res["segment-length"] <= self.speaker_change[0][0]:
                # asr segment inside speaker segment
                res["speaker"] = self.speaker_change[0][1]
                self.full_result_handler(res)
            else:
                res1, res2 = self._split_result_on_speaker_change(res, self.speaker_change[0])
                if res1:
                    # we can return first half of the result
                    self.full_result_handler(res1)
                if res2:
                    # read part after speaker change to buffer
                    self.result_buffer.appendleft(res2)
                # drop speaker change
                self.speaker_change.popleft()

    def _split_result_on_speaker_change(self, res, speaker_change):

       def word_before_change(word_start, word_length, speaker_change):
           # calculate time after speaker change
           time_after = word_start + word_length - speaker_change
           if time_after > 0 and time_after/word_length > 0.5:
               # if more than a half of word is after speaker change
               return False
           else:
               # otherwise 
               return True
          
       res1 = res.copy()
       # leave only best hyp
       res1["result"]["hypotheses"] = [res1["result"]["hypotheses"][0].copy()]

       res2 = res1.copy()
       hyp1 = []
       ali1 = []
       hyp2 = []
       ali2 = []

       # split alignments
       for w in res1["result"]["hypotheses"][0]["word-alignment"]:
           if word_before_change(w["start"] + res1["segment-start"], w["length"], speaker_change[0]):
               hyp1.append(w["word"])
               ali1.append(w)
           else:
               hyp2.append(w["word"])
               if ali1:                  
                   w["start"] -= ali1[-1]["start"] + ali1[-1]["length"]
               ali2.append(w)

       # create first part of split
       if hyp1: 
           res1["result"]["hypotheses"][0]["transcript"] = " ".join(hyp1)
           res1["result"]["hypotheses"][0]["word-alignment"] = ali1
           res1["segment-length"] = ali1[-1]["start"] + ali1[-1]["length"]
           res1["total-length"] = res1["segment-start"] + res1["segment-length"]
           res1["speaker"] = speaker_change[1]
       else:
           res1 = None

       # create second part of split
       if hyp2:
           res2["result"] = {"hypotheses": [{"transcript": " ".join(hyp2),
                                            "word-alignment": ali2}],
                             "final": True}
           if res1:
               res2["segment-start"] = res1["segment-start"] + res1["segment-length"]
               res2["segment-length"] -= res1["segment-length"]
       else:
           res2 = None

       return res1, res2

    def _on_new_sample(self, appsink):
        sample = appsink.emit('pull-sample')
        buf = sample.get_buffer()
        data = buf.extract_dup(0, buf.get_size())
        # call sample handler
        if self.diariz:
            self.diariz.on_new_sample(data)

        return Gst.FlowReturn.OK

    def _on_appsink_preroll(self, appsink):
        sample = appsink.emit('pull-preroll')
        # pull preroll, but do nothing

        return Gst.FlowReturn.OK

    def _on_appsink_eos(self, appsink):
        logger.debug('%s: Appsink received eos signal' % self.request_id)
        if self.diariz:
            self.diariz.on_eos()

    def _on_partial_result(self, asr, hyp: str) -> None:
        logger.info("%s: Got partial result: %s"
                    % (self.request_id, hyp))

        # check if we have too old final results in buffer
        while self.result_buffer and \
              self.result_buffer[0]["segment-start"] > self.current_segment[0] + self.diariz_conf.get("latency", 0.5):
            res = self.result_buffer.popleft()
            res["speaker"] = self.current_segment[2] if self.current_segment else "speaker0"
            if self.diariz_conf.get("as_partial_vad", False):
                res["result"]["hypotheses"] = [{"transcript": "", "word-alignment": []}]
            self.full_result_handler(res)

        if self.result_handler:
            self.result_handler(hyp, False)

    def _on_final_result(self, asr, hyp: str, like, conf) -> None:
        logger.info("%s: Got final results: %s %f %f"
                    % (self.request_id, hyp, like, conf))

        if self.result_handler:
            self.result_handler(hyp, True)

    def _on_full_final_result(self, asr, result_json: str) -> None:
        logger.debug("%s: Got full final result: %s"
                     % (self.request_id, result_json))

        if self.full_result_handler:
            # if diarization is enabled, place result in deque
            # and wait for diarization result
            if self.diariz:
                self.result_buffer.append(json.loads(result_json))
                # in some cases diarization maybe already done,
                # check if we can output results
                self._output_delayed_results()
            else:
                self.full_result_handler(json.loads(result_json))

    def _on_error(self, bus, msg) -> None:
        self.error = msg.parse_error()
        logger.error(self.error)
        self.finish_request()
        if self.error_handler:
            self.error_handler(self.error[0].message)

    def _on_eos(self, bus, msg) -> None:
        logger.info("%s: Pipeline received eos signal"
                    % self.request_id)

        self.finish_request()
        if self.eos_handler:
            self.eos_handler[0](self.eos_handler[1])

    def finish_request(self) -> None:
        if self.outdir:
            self.filesink.set_state(Gst.State.NULL)
            self.filesink.set_property("location", "/dev/null")
            self.filesink.set_state(Gst.State.PLAYING)
        self.pipeline.set_state(Gst.State.NULL)
        self.request_id = "<undefined>"

    def init_voiceprints(self, voiceprints: Dict) -> None:
        logger.info("%s: Initializing speaker xvectors" % self.request_id)

        if not self.diariz:
            logger.error("%s: Voiceprints sent with request, but worker is not configured to do speaker id!")
            return
        
        self.diariz.init_speakers(voiceprints)
           
    def init_request(self, id: str, caps_str: str) -> None:
        self.request_id = id
        logger.info("%s: Initializing request" % self.request_id)

        # prepare for diarization
        if self.diariz:
            self.result_buffer: deque[Any] = deque()
            self.current_segment = None
            self.speaker_change: deque[Any] = deque()
            self.diariz_done = False

        # create new decodebin
        self.decodebin.unlink(self.audioconvert)
        self.appsrc.unlink(self.decodebin)
        self.pipeline.remove(self.decodebin)
        self.pipeline.remove(self.appsrc)
        self.appsrc = Gst.ElementFactory.make("appsrc", "appsrc")
        self.appsrc.set_property("is-live", True)
        self.decodebin = Gst.ElementFactory.make("decodebin", "decodebin")
        self.pipeline.add(self.decodebin)
        self.pipeline.add(self.appsrc)
        self.appsrc.link(self.decodebin)
        self.decodebin.connect('pad-added', self._connect_decoder)

        if caps_str and len(caps_str) > 0:
            logger.info("%s: Setting caps to %s" % (self.request_id, caps_str))
            caps = Gst.caps_from_string(caps_str)
            self.appsrc.set_property("caps", caps)
        else:
            self.appsrc.set_property("caps", None)

        if self.outdir:
            self.pipeline.set_state(Gst.State.PAUSED)
            self.filesink.set_state(Gst.State.NULL)
            self.filesink.set_property('location',
                                       "%s/%s.raw" % (self.outdir, id))
            self.filesink.set_state(Gst.State.PLAYING)

        self.pipeline.set_state(Gst.State.PLAYING)
        self.filesink.set_state(Gst.State.PLAYING)
        self.asrsink.set_state(Gst.State.PLAYING)

    def process_data(self, data) -> None:
        logger.debug('%s: Pushing buffer of size %d to pipeline'
                     % (self.request_id, len(data)))
        buf = Gst.Buffer.new_allocate(None, len(data), None)
        buf.fill(0, data)
        self.appsrc.emit("push-buffer", buf)
        logger.debug('%s: Pushing buffer done' % self.request_id)

    def end_request(self) -> None:
        logger.info("%s: Pushing EOS to pipeline"
                    % self.request_id)
        self.appsrc.emit("end-of-stream")

    def set_word_handler(self, handler: Any) -> None:
        self.word_handler = handler

    def set_result_handler(self, handler: Any) -> None:
        self.result_handler = handler

    def set_full_result_handler(self, handler: Any) -> None:
        self.full_result_handler = handler

    def set_eos_handler(self,
                        handler: Any,
                        user_data: Optional[Any] = None) -> None:
        self.eos_handler = (handler, user_data)

    def set_error_handler(self, handler) -> None:
        self.error_handler = handler

    def cancel(self) -> None:
        logger.info("%s: Cancelling pipeline" % self.request_id)
        self.appsrc.emit("end-of-stream")

        logger.info("%s: Cancelled pipeline" % self.request_id)
