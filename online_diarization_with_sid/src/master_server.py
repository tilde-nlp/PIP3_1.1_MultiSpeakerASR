import logging
import logging.config
import asyncio
import yaml
import tornado.web
import hashlib
import aio_pika
import tornado.platform.asyncio
import tornado.ioloop
import tornado.httpserver
from tornado.ioloop import IOLoop
from gi.repository import GObject
import subprocess
import os
import ssl
import json
import _thread
import status as common
from jobs.rabbit_wrapper import RabbitWrapper, create_wrapper
from master.voiceprint.voiceprint_request_handler import VoiceprintRequestHandler
from master.offline.offline_result_handler import JobsHandler
from master.online.stream_request_handler import StreamRequestHandler

from typing import Optional, Dict, Any, List, Union

logger = logging.getLogger(__name__)


class MasterServer(tornado.web.Application):

    def __init__(self,
                 conf: Dict,
                 rabbit_wrapper: RabbitWrapper,
                 internal_address: str,
                 loop: asyncio.AbstractEventLoop):
        from tornado.options import options

        # copy config
        self.conf: Dict = conf

        loop.run_until_complete(self.load_config())

        settings = dict(
            cookie_secret="43oETzKXQAGaYdkL5gEmGeJJFuYh7EQnp2XdTP1o/Vo=",
            template_path=os.path.join(os.path.dirname(os.path.dirname(__file__)), "templates"),
            static_path=os.path.join(os.path.dirname(os.path.dirname(__file__)), "static"),
            xsrf_cookies=False,
            autoescape=None,
            out_dir=options.out_dir,
            shared_dir=options.shared_dir,
            acme_dir=options.acme_dir,
            etc_conf=self.etc_conf,
        )

        # make sure out_dir exists, if it's not the same as shared_dir
        if settings["out_dir"] != settings["shared_dir"]:
            out_dir_abs = "%s/%s" % (os.getcwd(), settings["out_dir"])
            if not os.path.isdir(out_dir_abs):
                logger.info("Creating local output directory: %s" % out_dir_abs)
                os.mkdir(out_dir_abs)

        handlers = [
            # offline recognition jobs, statuses, and also cleanup via DELETE
            (r"/client/jobs/(.*)", JobsHandler, {"path": settings["shared_dir"]}),
            # voiceprints
            (r"/client/voiceprint", VoiceprintRequestHandler),
        ]

        tornado.web.Application.__init__(self, handlers, **settings)
        self.rabbit_wrapper = rabbit_wrapper
        self.internal_address = internal_address
        # used to map online asr clients to online asr workers
        self.client_job_map: Dict[str, Any] = dict()

        self.max_filesize = options.max_filesize

    # noinspection PyAttributeOutsideInit
    async def load_config(self) -> None:
        from tornado.options import options

        with open(options.conf, encoding="utf-8") as f:
            self.etc_conf: Dict[str, Any] = yaml.safe_load(f)

        self.auth_key: str = self.etc_conf["auth_key"]
        self.stats_key: Optional[str] = self.etc_conf.get("stats_key")

        # load logging config
        if "logging" in self.etc_conf:
            logging.config.dictConfig(self.etc_conf["logging"])

    # set_job_status writes status file to disk for given request_id
    #   and sends email update and updates journal
    async def set_job_status(self,
                             job: Dict[str, Any],
                             status_code: int,
                             other: Optional[Dict[str, Any]] = None) \
            -> None:
        if other:
            response = other.copy()
        else:
            response = dict()
        response['status'] = status_code
        response['request_id'] = job['id']
        if "length" in job:
            response['length'] = job['length']

        status_path = "%s/%s/%s.status" \
                      % (os.getcwd(), self.settings["shared_dir"], job["id"])
        json.dump(response, open(status_path, 'w', encoding="utf-8"))

    def test_auth_string(self, authstr: str, hashstr: str) -> bool:
        if not self.auth_key:
            return True
        return hashlib.sha1((authstr + self.auth_key)
                            .encode("utf-8")).hexdigest() == hashstr

    def validate_asr_system(self, request_id: str, system: str, offline: bool) -> str:
        """
        Validates that the requested asr system is supported.
        In case such a system is not supported, the default system is used instead.
        """
        system_conf: Dict[str, Union[str, List[str]]] = self.etc_conf["systems"]
        if offline:
            if system not in system_conf["valid_offline"]:
                logger.warning("%s: specified offline system=%s is not valid, using default instead"
                                % (request_id, system))
                return system_conf["default_offline_system"]
        else:
            if system not in system_conf["valid_online"]:
                logger.warning("%s: specified online system=%s is not valid, using default instead"
                                % (request_id, system))
                return system_conf["default_online_system"]
        return system


def define_global_settings() -> None:
    from tornado.options import define

    define("port", default=80, help="run on the given port", type=int)
    define("ssl_port", default=443, help="run on the given SSL port", type=int)
    define("max_filesize", default=100, help="max file length in megabytes", type=int)
    define("out_dir", default='tmp_local', help="temporary storage for files", type=str)
    define("shared_dir", default="tmp_shared", help="shared storage for converted audio files and transcriptions", type=str)
    define("acme_dir", default='.well-known/acme-challenge', help="directory for acme challenges", type=str)
    define("conf", default='config.yaml', help="configuration", type=str)


async def setup_rabbit(loop: asyncio.AbstractEventLoop,
                       config: Dict[str, str]) -> RabbitWrapper:
    amqp_url = config['amqp_url']
    app_id = config["app_id"]
    exchange_name = config["exchange_name"]
    # we use separate queues per system, but single exchange
    # queue names are the same as their routing keys
    jobs_ee_offline_general = config["jobs_ee_offline_general"]
    jobs_ee_online_general = config["jobs_ee_online_general"]
    jobs_voiceprint = config.get("jobs_voiceprint", "voiceprint")
    # result queue is used to pass results for offline asr for ALL systems
    results_queue_name = config["results_queue_name"]

    # set up wrapper and declare everything we know about
    rabbit_wrapper = await create_wrapper(loop,
                                          amqp_url,
                                          app_id)
    await rabbit_wrapper.declare_exchange(exchange_name)

    async def declare_and_bind(queue_name: str):
        await rabbit_wrapper.declare_queue(queue_name)
        await rabbit_wrapper.bind_queue(exchange_name, queue_name, queue_name)

    # offline asr
    await declare_and_bind(jobs_ee_offline_general)
    # voiceprint
    await declare_and_bind(jobs_voiceprint)
    # offline asr results
    await declare_and_bind(results_queue_name)
    # online asr
    await declare_and_bind(jobs_ee_online_general)

    return rabbit_wrapper


async def subscribe_to_results(wrapper: RabbitWrapper,
                               app: MasterServer) -> None:
    logger.info("Subscribing to result queue")
    queue_name = "results"
    async with wrapper.get_queue_iter(queue_name) as queue_iter:
        async for message in queue_iter:  # type: aio_pika.IncomingMessage
            async with message.process(requeue=True, ignore_processed=True):
                logger.info("Processing result message: %s" % message.body)
                status_msg = json.loads(message.body)
                await app.set_job_status(job=status_msg,
                                         status_code=status_msg["status"])
                await message.ack()


# this method has to work inside ubuntu-based containers, running on k8s
def get_internal_ip() -> str:
    output = subprocess.check_output(["hostname", "--ip-address"])
    output = output.decode().strip()
    output = output.split(' ', maxsplit=1)
    return output[0]


def main() -> None:
    # create event loops on all threads to match python2/tornado5 behavior
    asyncio.set_event_loop_policy(
        tornado.platform.asyncio.AnyThreadEventLoopPolicy())

    define_global_settings()

    logging.basicConfig(level=logging.DEBUG,
                        format="%(levelname)8s %(asctime)s %(message)s ")
    logger.info('Starting up server')

    from tornado.options import options
    options.parse_command_line()

    # load conf
    with open(options.conf, encoding="utf-8") as f:
        conf = yaml.safe_load(f)

    # find out our ip
    # this is used for guiding online asr workers to the right master
    internal_address = "%s:%s" % (get_internal_ip(), options.port)

    # inside k8s, amqp address is specified as an env variable
    amqp_url = os.getenv("AMQP_URL")
    if amqp_url:
        conf["rabbitmq"]["amqp_url"] = amqp_url

    loop = asyncio.get_event_loop()
    rabbit_wrapper = loop.run_until_complete(setup_rabbit(loop, conf["rabbitmq"]))

    app = MasterServer(conf, rabbit_wrapper, internal_address, loop)
    loop.create_task(subscribe_to_results(rabbit_wrapper, app))

    # run HTTPS server, if we have config
    if conf.get('ssl_certfile', None) and conf.get('ssl_keyfile', None):
        # start HTTPS
        ssl_ctx = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
        ssl_ctx.load_cert_chain(conf['ssl_certfile'],
                                conf['ssl_keyfile'])
        https_server = tornado.httpserver.HTTPServer(app, ssl_options=ssl_ctx)

        https_server.bind(options.ssl_port)
        https_server.start(1)

    # start HTTP
    http_server = tornado.httpserver.HTTPServer(app)
    http_server.bind(options.port)
    http_server.start(1)

    loop = GObject.MainLoop()
    _thread.start_new_thread(loop.run, ())

    logger.info("Starting main IOLoop")
    IOLoop.current().start()


if __name__ == "__main__":
    main()
