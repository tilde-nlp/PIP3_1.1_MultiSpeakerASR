import asyncio
import aio_pika
import logging
import logging.config
import json
import os
import argparse
import yaml
import time
import status
import signal
import system_loader
import shutil

from jobs import Job
from jobs.rabbit_wrapper import RabbitWrapper, create_wrapper
from worker.online.diart_xvector_extractor import XVectorExtractor

from typing import Dict, Any, List, Optional

logger = logging.getLogger(__name__)
sigterm_called = False
# tells us, whether we are currently busy processing stuff
# used for handling SIGTERM ignore, until this is False
currently_processing = False


class ExtractorWrapper(object):
    def __init__(self,
                 loop: asyncio.AbstractEventLoop,
                 rabbit_wrapper: RabbitWrapper,
                 system_dir: str,
                 decode_dir: str,
                 xvector_conf: dict,
                 do_clean: bool,
                 result_queue: str,
                 exchange_name: str,
                 n_threads: int = 1):
        self.loop = loop
        self.rabbit_wrapper = rabbit_wrapper

        # conf
        self.system_dir = system_dir
        self.decode_dir = decode_dir
        self.do_clean = do_clean
        self.n_threads = n_threads
        self.result_queue = result_queue
        self.exchange_name = exchange_name
        self.xvector_conf = xvector_conf
        # we need decode_dir to be absolute
        if not os.path.isabs(self.decode_dir):
            self.decode_dir = "%s/%s" % (os.getcwd(), self.decode_dir)

        # state
        # per-request state
        self.job: Optional[Job] = None
        # noinspection PyTypeChecker
        self.decoding: asyncio.subprocess.Process = None
        self.decoding_finished = False

    async def _update_status(self, status_code: int):
        msg = self.job
        msg["status"] = status_code
        await self.rabbit_wrapper.publish_json(self.exchange_name, self.result_queue, msg)

    async def process_message(self,
                                  msg: bytes) -> bool:
        self.job: Job = json.loads(msg.decode("utf-8"))
        request_id = self.job["id"]
        # update status that we've started decoding this job
        await self._update_status(status.STATUS_PROCESSING)
        
        xvector = await self.extract_xvector()

        if not xvector:
            # extraction failed
            await self._update_status(status.STATUS_ABORTED)
            # return True to avoid reschedule
            return True

        result_json = {"xvector": xvector.decode("utf-8")}

        json_name = "%s/%s.json" \
                        % (self.decode_dir, request_id)
        json.dump(result_json, open(json_name, "w", encoding="utf-8"))
        logger.info("%s: Voiceprint decoding complete." % request_id)
        await self._update_status(status.STATUS_SUCCESS)

        # finally, when we return, main_loop will ack the message
        return True

    async def extract_xvector(self) -> bool:
        src_file = "%s/%s.wav" % (self.decode_dir, self.job["id"])

        extractor = XVectorExtractor(self.xvector_conf)
        voiceprint = extractor.get_voiceprint_from_file(src_file)

        return voiceprint


async def worker_loop(loop: asyncio.AbstractEventLoop,
                      conf: Dict[str, Any]) -> None:
    rabbit_conf: Dict[str, str] = conf["rabbitmq"]
    amqp_url = rabbit_conf["amqp_url"]
    exchange_name = rabbit_conf["exchange_name"]
    # we use jobs_queue_name as routing_key and app_id as well, for simplicity
    jobs_queue_name = rabbit_conf["jobs_queue_name"]
    # we use results_queue_name as routing_key as well
    results_queue_name = rabbit_conf["results_queue_name"]

    rabbit_wrapper: RabbitWrapper = await create_wrapper(loop,
                                                         amqp_url,
                                                         app_id=jobs_queue_name)
    await rabbit_wrapper.declare_exchange(exchange_name)
    # job queue
    await rabbit_wrapper.declare_queue(jobs_queue_name)
    await rabbit_wrapper.bind_queue(exchange_name,
                                    jobs_queue_name,
                                    jobs_queue_name)
    # result/status queue
    await rabbit_wrapper.declare_queue(results_queue_name)
    await rabbit_wrapper.bind_queue(exchange_name,
                                    results_queue_name,
                                    results_queue_name)
    # create decoder wrapper
    extractor_wrapper = ExtractorWrapper(loop=loop,
                                     rabbit_wrapper=rabbit_wrapper,
                                     system_dir=conf["system-dir"],
                                     decode_dir=conf["decode-dir"],
                                     xvector_conf=conf["xvector"],
                                     result_queue=results_queue_name,
                                     exchange_name=exchange_name,
                                     do_clean=conf["clean-decode-files"],
                                     n_threads=conf["n-threads"])

    async with rabbit_wrapper.get_queue_iter(jobs_queue_name) as queue_iter:
        async for message in queue_iter:  # type: aio_pika.IncomingMessage
            async with message.process(requeue=True, ignore_processed=True):
                global currently_processing, sigterm_called
                currently_processing = True

                logger.info("Processing message: %s" % message.body)
                processed_ok = await extractor_wrapper.process_message(message.body)
                if processed_ok:
                    await message.ack()
                else:
                    await message.nack()

                currently_processing = False
                if sigterm_called:
                    logger.info("Worker was scheduled to terminate. Terminating...")
                    exit(0)


def sigterm_handler(_signal, _frame) -> None:
    logger.info("Received SIGTERM")
    global sigterm_called, currently_processing
    logger.info("currently_processing = %s" % currently_processing)
    sigterm_called = True
    # if we are not doing anything, terminate immediately
    if not currently_processing:
        exit(0)
    # otherwise message processing loop will get to sigterm check and terminate


def main() -> None:
    log_format = ("%(levelname) -10s %(asctime)s %(name) -20s %(funcName) "
                  "-15s %(lineno) -5d: %(message)s")
    # below is old worker format
    # log_format = "%(levelname)8s %(asctime)s %(message)s "
    logging.basicConfig(level=logging.DEBUG, format=log_format)
    logger.debug("Starting up voiceprint worker")

    parser = argparse.ArgumentParser(description="Voiceprint worker")
    parser.add_argument("-c", "--conf", dest="conf", required=True,
                        help="YAML file with decoder configuration")
    args = parser.parse_args()

    with open(args.conf) as f:
        conf: Dict[str, Any] = yaml.safe_load(f)

    # load the right asr system depending on our profile.
    # this reads the latest configuration for this type of system from the
    # shared model directory, and then symlinks the right models into
    # our local transcriber directory, enabling us to do ivector extraction with desired models.
    target_system_name = conf["profile"]["type"]
    system_loader.load_offline_system(target_transcriber_dir=conf["system-dir"],
                                      system_name=target_system_name,
                                      model_dir=conf["model-dir"],
                                      copy_instead=False)

    # reading rabbitmq url from env is easier inside k8s
    amqp_url = os.getenv("AMQP_URL")
    if amqp_url:
        conf["rabbitmq"]["amqp_url"] = amqp_url
    else:
        logger.warning("Using default amqp_url")

    # create handler for sigterm
    signal.signal(signal.SIGTERM, sigterm_handler)

    loop = asyncio.get_event_loop()
    loop.run_until_complete(worker_loop(loop, conf))


if __name__ == "__main__":
    main()
