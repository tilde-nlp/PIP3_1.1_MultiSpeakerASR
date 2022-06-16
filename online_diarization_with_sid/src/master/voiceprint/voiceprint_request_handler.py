import struct
import shutil
import tornado.web
import tornado.locks
import tornado.escape
import uuid
import logging
import json
import status
import os
from jobs.rabbit_wrapper import RabbitWrapper
from collections import defaultdict
from tornado.ioloop import IOLoop
from master.offline.multipart_parser import MultipartParser
from master.offline.gst_converter import ConverterPipeline, ConverterBufferedPipeline

from typing import Optional, Dict, List, Awaitable, Any

logger = logging.getLogger(__name__)


# noinspection PyAbstractClass
@tornado.web.stream_request_body
class VoiceprintRequestHandler(tornado.web.RequestHandler):
    """
    Provides an HTTP POST interface supporting chunked transfer requests
    """

    def _test_auth(self) -> bool:
        headers = self.request.headers
        if headers:
            auth_header = headers.get("ASR-auth")
            if auth_header:
                return auth_header == self.application.auth_key
        return False

    # noinspection PyAttributeOutsideInit
    async def prepare(self) -> Optional[Awaitable[None]]:
        if self.request.method == "OPTIONS":
            # OPTIONS call, no need to init job
            return

        self.id: str = str(uuid.uuid4())
        self.job: Optional[Dict[str, Any]] = None
        self.field: Dict[str, Any] = defaultdict(str)
        self.gst_error = False
        self.error_status = 0
        self.error_message: Optional[str] = None
        self.ioloop = IOLoop.current()  # save ref to current IOLoop
        self.convert_done = tornado.locks.Event()
        self.converter = None
        self.boundary = None
        # request args
        # try to read voice from path args

        logger.info("%s: OPEN: system_type='voiceprint'" % (
            self.id))

        # check auth
        if not self._test_auth():
            logger.error("%s: auth failed." % self.id)
            self.gst_error = True  # this skips GST conversion
            response = {"status": status.STATUS_NOAUTH,
                        "request_id": self.id}
            # error messages allowed for all origins
            self.set_header('Access-Control-Allow-Origin', "*")
            self.set_status(401)
            self.write(json.dumps(response, ensure_ascii=False))
            await self.finish()
            return

        # check if we are receiving a multi-part request
        content_type: Optional[str] = self.request.headers.get("Content-Type", None)
        if content_type and content_type.startswith("multipart"):
            # extract the multipart boundary
            self.boundary = None
            fields = content_type.split(";")
            for field in fields:
                k, sep, v = field.strip().partition("=")
                if k == "boundary" and v:
                    if v.startswith('"') and v.endswith('"'):
                        self.boundary = tornado.escape.utf8(v[1:-1])
                    else:
                        self.boundary = tornado.escape.utf8(v)
                    break
            content_type = None
        else:
            response = {"status": status.STATUS_NOT_ALLOWED, "request_id": self.id}
            logger.warning("%s: can't recognize a non-multipart request" % self.id)
            self.set_header("Access-Control-Allow-Origin", "*")
            self.write(json.dumps(response))
            self.set_status(400)
            await self.finish()
            return

        # all checks passed, proceed with request
        self.content_type = content_type
        max_file_size = 100 # limit max voice sample size to 100MB (typically, it will be few MB only)
        logger.info("%s: Setting max body size to %sMB for this request" %
                    (self.id, max_file_size))
        self.request.connection.set_max_body_size(max_file_size * 1024 * 1024)
        self.multipart_parser = MultipartParser(self.id,
                                                self,
                                                self.boundary,
                                                max_file_size)

    async def data_received(self, chunk: bytes) -> Optional[Awaitable[None]]:
        if self.gst_error:
            return

        # if not multipart/form-data, then just forward directly
        if not self.boundary:
            logger.debug("%s: Forwarding %d bytes to converter" % (self.id, len(chunk)))
            self.converter.process_data(chunk)
            return

        try:
            # exception style control flow, sigh...
            self.multipart_parser.receive_chunk(chunk)
        except ValueError as e:
            self._on_error("", repr(e))

    def process_data(self, chunk: bytes) -> None:
        """
        This function is called by MultipartParser.
        """
        if not self.converter:
            self.field = self.multipart_parser.field
            self.converter = ConverterBufferedPipeline(self.settings)
            self.converter.set_result_handler(self._on_result)
            self.converter.set_error_handler(self._on_error)
            self.converter.init_request(self.id, self.content_type)
            logger.info("%s: Receiving and converting audio" % self.id)
        self.converter.process_data(chunk)

    async def post(self, *args, **kwargs) -> None:
        logger.info("%s: Handling the end of chunked recognize request" % self.id)

        # check for gst errors
        if self.gst_error:
            self.set_header("Access-Control-Allow-Origin", "*")
            await self.finish()
            # just finish, error has been already reported back
            return

        # handling unfinished business
        self.multipart_parser.finish()

        self.field = self.multipart_parser.field
        # inject parsed multipart arguments in to request object
        for k, v in self.field.items():
            self.request.body_arguments[k] = [v]

        assert self.converter is not None
        self.converter.end_request()
        logger.info("%s: waiting for GST to finish..." % self.id)
        await self.convert_done.wait()

        # check for gst errors
        if self.gst_error:
            # request is finished in _on_error handler
            # just do nothing and exit
            return

        if self.error_status == 0:
            logger.info("%s: Transfer & convert done" % self.id)

            # start job
            await self._start_job()

            # return URI of created job
            self.set_header('Location', "file/%s/status" % self.id)

            response = {"status": 0,
                        "request_id": self.id}
        else:
            logger.error(
                "%s: Error (status=%d) processing HTTP request: %s"
                % (self.id, self.error_status, self.error_message))
            if self.error_status == status.STATUS_NOT_ALLOWED:
                self.set_status(503)
            else:
                self.set_status(400)
            response = {"status": self.error_status,
                        "request_id": self.id,
                        "message": self.error_message}

        self.set_header('Access-Control-Allow-Origin', "*")
        self.write(json.dumps(response, ensure_ascii=False))
        await self.finish()

    def _on_error(self, request: str, error: str) -> None:
        """
        This is called by converter, when an error occurs.
        """
        logger.error("%s: Error while converting audio: %s"
                     % (self.id, error))
        job = {"id": self.id,
               "etc": {"email": self.field['email']}}

        # ignore double errors
        if self.gst_error:
            return

        # set error state
        self.gst_error = True
        if error == "Conversion time limit reached":
            self.error_status = status.STATUS_TIMEOUT
            self.error_message = "File could not be converted in time."
        else:
            self.error_status = status.STATUS_NOT_ALLOWED
            self.error_message = "File type not allowed"

        self.ioloop.spawn_callback(self.application.set_job_status, job, self.error_status)
        response = {"status": self.error_status, "request_id": self.id}
        self.set_header('Access-Control-Allow-Origin', '*')
        self.write(json.dumps(response))
        # finish
        self.finish()

        # release waiting lock
        self.ioloop.add_callback(self.convert_done.set)

    def _on_result(self, result: str) -> None:
        """
        This is called by converter, when things are done
        """
        # move locally converted files to shared storage
        prefix_out = "%s/%s" % (os.getcwd(), self.application.settings["out_dir"])
        prefix_shared = "%s/%s" % (os.getcwd(), self.application.settings["shared_dir"])
        # move mp3
        src_mp3 = "%s/%s.mp3" % (prefix_out, self.id)
        tgt_mp3 = "%s/%s.mp3" % (prefix_shared, self.id)
        shutil.move(src_mp3, tgt_mp3)
        # move converted wav
        src_wav = "%s/%s.wav" % (prefix_out, self.id)
        tgt_wav = "%s/%s.wav" % (prefix_shared, self.id)
        shutil.move(src_wav, tgt_wav)

        # add job
        logger.info("%s: Conversion finished. Creating decoding job %s"
                    % (self.id, result))
        self._create_job()

        # unlock main flow (file have been processed)
        self.ioloop.add_callback(self.convert_done.set)

    def _create_job(self) -> None:

        logger.info("%s: system_type='voiceprint'" % (
               self.id))

        job: Dict[str, Any] = {"id": self.id,
                               "api": "voiceprint_api",
                               "ip-address": self.request.remote_ip,
                               "user-agent": self.request.headers.get("User-Agent", ""),
                               }

        self.job = job

    async def _start_job(self) -> None:
        if not self.job:
            logger.warning("%s: Attempted to start an empty job!" % self.id)
            return

        await self.application.set_job_status(self.job, status.STATUS_SCHEDULED)
        # pass job to rabbit
        rabbit_wrapper: RabbitWrapper = self.application.rabbit_wrapper
        # hardcoded exchange path, but we only use 1 exchange for everything, so this is ok
        await rabbit_wrapper.publish_json(exchange_name=self.application.conf["rabbitmq"]["exchange_name"],
                                          routing_key=self.application.conf["rabbitmq"].get("jobs_voiceprint","voiceprint"),
                                          msg=self.job)

        self.job = None

    def on_finish(self) -> None:
        if self.converter:
            self.converter.destroy()

    # CORS stuff
    def options(self, *args, **kwargs) -> None:
        self.set_header('Access-Control-Allow-Origin', '*')
        self.set_header('Access-Control-Allow-Headers', 'accept, content-type, mode')
        if self.converter:
            self.converter.end_request()
        self.finish()
