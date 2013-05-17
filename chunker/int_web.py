import os
import web
import json

from chunker.core import Core
from chunker.args import WebArgumentParser, ArgParseException
from chunker.util import get_config_path


core = Core()
core._init_parser(WebArgumentParser)
core.start()


class api:
    def GET(self, cmd):
        try:
            args = core.parser.parse_args(cmd, web.input())
            data = args.func(args)
            return json.dumps(data, indent=4)
        except Exception as e:
            return json.dumps({"status": "error", "message": str(e)})


class index:
    def GET(self):
        return file("static/index.html").read()


class download:
    def GET(self, uuid, name):
        if uuid in core.repos:
            repo = core.repos[uuid]
            web.header('Content-Type', 'application/chunker') # file type
            web.header('Content-Disposition', 'attachment; filename=' + repo.name + ".chunker")
            return json.dumps(repo.to_struct(state=False))
        else:
            return "Can't find repo '%s'" % uuid


def main():
    os.chdir(os.path.dirname(__file__))
    app = web.application((
        '/api/(.*).json', 'api',
        '/api/(.*)', 'api',
        '/download/(.*)/(.*).chunker', 'download',
        '/', 'index',
    ), globals())
    app.run()
