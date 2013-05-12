import web
import json

from chunker.core import Core
from chunker.args import WebArgumentParser, ArgParseException


core = Core()
core._init_parser(WebArgumentParser)


class api:
    def GET(self, cmd):
        try:
            args = core.parser.parse_args(cmd, web.input())
            data = args.func(args)
            return json.dumps(data, indent=4)
        except Exception as e:
            return json.dumps({"status": "error", "message": str(e)})


class favicon:
    def GET(self):
        return ""


class index:
    def GET(self):
        return ""


def main():
    app = web.application((
        '/api/(.*)', 'api',
        '/favicon.ico', 'favicon',
        '/', 'index',
    ), globals())
    app.run()
