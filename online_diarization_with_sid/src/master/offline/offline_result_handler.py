import tornado.web
import os
import logging

logger = logging.getLogger(__name__)


# noinspection PyAbstractClass
class JobsHandler(tornado.web.StaticFileHandler):
    """
    this serves result files for offline recognition with GET.
    this also handles cleanup duties on DELETE.
    """

    def _test_auth(self) -> bool:
        headers = self.request.headers
        if headers:
            auth_header = headers.get("ASR-auth")
            if auth_header:
                return auth_header == self.application.auth_key
        return False

    async def delete(self, *args, **kwargs):
        target_id = args[0]
        logger.info("%s: Cleanup requested" % target_id)

        # clear up all the files belonging to this id
        def remove(extension: str) -> None:
            target_path = "%s/%s.%s" % (self.root, target_id, extension)
            if os.path.exists(target_path):
                os.remove(target_path)

        remove("json")
        remove("status")

        self.set_status(200)
        await self.finish()

    # noinspection PyAttributeOutsideInit
    def parse_url_path(self, path):
        # take the extension part
        self.output_format = path.split(".")[-1]
        self.set_header('Access-Control-Allow-Origin', '*')
        return super(JobsHandler, self).parse_url_path(path)

    def get_absolute_path(self, root, path):
        dst_path = super(JobsHandler, self).get_absolute_path(root, path)

        return dst_path

    def check_etag_header(self):
        if self.output_format:
            attachname = self.path
            self.set_header("Content-Disposition", "attachment; filename=\"%s\"" % attachname)
        return False
