import argparse


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
    def parse_args(self, path, params):
        args = []

        for part in path.split("/"):
            args.append(str(part))

        for key, value in params.items():
            if value == "on":  # we assume that "on" is a boolean flag
                args.append("--"+key)
            else:
                args.append("--"+key)
                args.append(value)


        print args
        return argparse.ArgumentParser.parse_args(self, args)
