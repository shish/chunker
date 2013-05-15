import argparse
import urlparse


class ArgParseException(Exception):
    pass


class NonExitingArgumentParser(argparse.ArgumentParser):
    def exit(self, status=0, message=None):
        raise ArgParseException(message)


class WebArgumentParser(NonExitingArgumentParser):
    """
    Turns HTTP GET requests into argparse-able command lines

    http://mysite.com/api/foo/bar?pie=123&cake=456

    sys.args = ["foo", "bar", "--pie", "123", "--cake", "456"]

    common.py:
       def getParser(base):
           parser = base()
           # do typical argparse setup here
           return parser

    my-cli.py:
       while True:
           raw_args = raw_input("type a command >> ")
           args = getParser(ArgumentParser).parse(raw_input.split())
           print args.func(args)

    my-web.py
       def GET(request):
           args = getParser(WebArgumentParser).parse(request.path, request.GET)
           return args.func(args)
    """
    def path_params_to_args(self, path, params):
        args = []

        for part in path.split("/"):
            args.append(str(part))

        for key, value_list in params.items():
            for value in value_list:
                if value == "on":  # we assume that "on" is a boolean flag
                    args.append("--"+key)
                else:
                    args.append("--"+key)
                    args.append(value)

        return args

    def url_to_args(self, url, extra_params):
        parts = urlparse.urlparse(url)
        path = parts.path.strip("/")
        params = urlparse.parse_qs(parts.query)
        params.update(extra_params)
        return self.path_params_to_args(path, params)

    def parse_args(self, url, params={}):
        args = self.url_to_args(url, params)
        return argparse.ArgumentParser.parse_args(self, args)
