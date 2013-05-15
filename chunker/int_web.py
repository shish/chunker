import os
import web
import json

from chunker.core import Core
from chunker.args import WebArgumentParser, ArgParseException


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


def main():
    os.chdir(os.path.dirname(__file__))
    app = web.application((
        '/api/(.*).json', 'api',
        '/api/(.*)', 'api',
        '/', 'index',
    ), globals())
    app.run()
