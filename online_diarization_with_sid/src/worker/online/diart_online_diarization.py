"""
Pipeline for online speaker diarization using diart

Created on May 3rd, 2022

@author: Askar Salimbajevs
"""
import logging

from pyannote.core import Annotation, Segment, SlidingWindowFeature
from rx.core import Observer
import rx
import rx.operators as ops
import diart.blocks as blocks
import diart.operators as dops
from diart.pipelines import OnlineSpeakerDiarization, PipelineConfig
from diart.sources import AudioSource
import numpy as np
import base64

from typing import Optional, Any, Dict, List
from typing import Tuple, Text

logger = logging.getLogger(__name__)


class DiartOnlineDiarization(object):
    def __init__(self, conf: Optional[Dict] = None) -> None:
        self.result_handler = None
        self.subscription = None

        # Define online speaker diarization pipeline
        self.pipeline = OnlineSpeakerDiarizationWithSID(PipelineConfig(
                                                 segmentation = conf.get("segmentation", "pyannote/segmentation"),
                                                 embedding = conf.get("embedding", "pyannote/embedding"),
                                                 step=conf.get("step",0.5),
                                                 latency=conf.get("latency",0.5),
                                                 tau_active=conf.get("tau",0.5),
                                                 rho_update=conf.get("rho",0.3),
                                                 delta_new=conf.get("delta",1.0),
                                                 gamma=conf.get("gamma",3.0),
                                                 beta=conf.get("beta",10.0),
                                                 max_speakers=conf.get("max_speakers",20),
                                                 device=torch.device("cuda") if conf.get("gpu", False) else None))

        self._preroll()

    def _preroll(self) -> None:
        if self.subscription is not None:
            self.subscription.dispose()

        self.audio_input = StreamAudioSource()

        self.subscription = \
            self.pipeline.from_source(self.audio_input, False).subscribe(on_next = self._on_diariz, 
                                                                         on_completed = self._on_completed,
                                                                         on_error = lambda e: logger.exception("Error {0}".format(e)))

        # pre-roll with 5 sec of empty audio
        for _ in range(10):
           # push 0.5 sec of silence
           self.audio_input.on_next(np.zeros([1, 8000],dtype=np.float32))

    def init_speakers(self, voiceprints):
        # pass xvectors to initialize clusters
        self.pipeline.init_sid(voiceprints)

    def __del__(self):
        if self.subscription is not None:
            self.subscription.dispose()

    def on_eos(self):
        self.audio_input.on_eos()

    def on_new_sample(self, buf):        
        buf = np.frombuffer(buf, dtype=np.dtype('<i2')) # S16LE sample format
        buf = buf.astype(np.float32, order='C') / 32768.0 # convert to float
        self.audio_input.on_next(buf.reshape([1,-1]))

    def set_diariz_handler(self, handler):
        self.result_handler = handler

    def _on_completed(self):
        # diarization completed
        if self.result_handler:
            self.result_handler((0, 0, "done"))

        # preroll to prepare for next input
        # temporary disable result handler
        handler = self.result_handler
        self.result_hander = None
        self._preroll()
        # restore handler
        self.result_handler = handler

    def _on_diariz(self, value: Tuple[Annotation, Optional[SlidingWindowFeature]]):
        if self.result_handler:
            for segment, _, label in value[0].itertracks(yield_label=True):
                # substract empty-preroll
                self.result_handler((segment.start-5, 
                                     segment.duration, 
                                     label))

class StreamAudioSource(AudioSource):

    def __init__(self, sample_rate: int = 16000):
        super().__init__("live_recording", sample_rate)

    def on_next(self, buf):
        self.stream.on_next(buf)

    def on_eos(self):
        self.stream.on_completed()
       

"""
Modified speaker diarization pipeline that performs speaker id
"""
class OnlineSpeakerDiarizationWithSID(OnlineSpeakerDiarization):
    def __init__(self, config: PipelineConfig):
        super().__init__(config)
        self.speaker_tracking = OnlineSpeakerTrackingWithSID(config)
    
    def init_sid(self, voiceprints):
        self.speaker_tracking.init_sid(voiceprints)


class OnlineSpeakerTrackingWithSID:
    def __init__(self, config: PipelineConfig):
        self.config = config

    def init_sid(self, voiceprints):
        # add known speakers 
        spk_map = {}
        try:
            for spk in voiceprints:
                name = spk["name"]
                xvector = base64.b64decode(spk["xvector"])
                xvector = np.frombuffer(xvector, dtype=np.float32)
                spk_id = self.clustering.add_center(xvector)
                spk_map[spk_id] = name
            # reverse mapping
            self.binarize.set_speakers(spk_map)
        except Exception as e:
            logger.exception("Failed to load voiceprints")

    def from_model_streams(
        self,
        uri: Text,
        source_duration: Optional[float],
        segmentation_stream: rx.Observable,
        embedding_stream: rx.Observable,
        audio_chunk_stream: Optional[rx.Observable] = None,
    ) -> rx.Observable:
        end_time = None
        if source_duration is not None:
            end_time = self.config.last_chunk_end_time(source_duration)
        # Initialize clustering and aggregation modules
        self.clustering = blocks.OnlineSpeakerClustering(
            self.config.tau_active,
            self.config.rho_update,
            self.config.delta_new,
            "cosine",
            self.config.max_speakers,
        )

        aggregation = blocks.DelayedAggregation(
            self.config.step, self.config.latency, strategy="hamming", stream_end=end_time
        )
        self.binarize = BinarizeWithSpeakerMap(uri, self.config.tau_active)

        # Join segmentation and embedding streams to update a background clustering model
        #  while regulating latency and binarizing the output
        pipeline = rx.zip(segmentation_stream, embedding_stream).pipe(
            ops.starmap(self.clustering),
            # Buffer 'num_overlapping' sliding chunks with a step of 1 chunk
            dops.buffer_slide(aggregation.num_overlapping_windows),
            # Aggregate overlapping output windows
            ops.map(aggregation),
            # Binarize output
            ops.map(self.binarize),
        )
        # Add corresponding waveform to the output
        if audio_chunk_stream is not None:
            window_selector = blocks.DelayedAggregation(
                self.config.step, self.config.latency, strategy="first", stream_end=end_time
            )
            waveform_stream = audio_chunk_stream.pipe(
                dops.buffer_slide(window_selector.num_overlapping_windows),
                ops.map(window_selector),
            )
            return rx.zip(pipeline, waveform_stream)
        # No waveform needed, add None for consistency
        return pipeline.pipe(ops.map(lambda ann: (ann, None)))


class BinarizeWithSpeakerMap(blocks.Binarize):

    def __init__(self, uri: str, tau_active: float):
        super().__init__(uri, tau_active)
        self.speakers = {}

    def set_speakers(self, speaker_map: Dict):
        self.speakers = speaker_map

    def __call__(self, segmentation: SlidingWindowFeature) -> Annotation:
        annotation = Annotation(uri=self.uri, modality="speech")
        for speaker in range(segmentation.data.shape[1]):
            turns = self._binarize(self._select(segmentation, speaker))
            for speaker_turn in turns.itersegments():
                annotation[speaker_turn, speaker] = self.speakers.get(speaker, f"speaker{speaker}")
        return annotation
