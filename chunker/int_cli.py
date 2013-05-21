#!/usr/bin/env python

import sys
import json
import os
import readline

from chunker.args import ArgParseException
from chunker.repo import Repo
from chunker.util import log
from chunker.net import MetaNet
from chunker.core import Core
from chunker.args import NonExitingArgumentParser, ArgParseException


class Main(Core):
    def __init__(self, args=[]):
        Core.__init__(self)
        self._init_parser(NonExitingArgumentParser)

        if args:
            try:
                print json.dumps(self.do(args), indent=4)
            except ArgParseException:
                pass
        else:
            self.main_loop()

    def main_loop(self):
        #self.mn = MetaNet(self.config)

        log("Activating repos")
        for repo in self.repos:
            log("Activating %s" % repo)
            repo.start()

        while True:
            try:
                cmd = raw_input("chunker> ")
                data = self.do(cmd.split())
            #except ArgParseException as e:
            #    print e.message
            except (KeyboardInterrupt, EOFError) as e:
                break
            except Exception as e:
                data = {"status": "error", "message": str(e)}
            print json.dumps(data, indent=4)

    def do(self, argv):
        args = self.parser.parse_args(argv)
        return args.func(args)


def main():
    Main(sys.argv[1:])

