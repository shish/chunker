#!/usr/bin/env python

import argparse
import sys
import json
import os
import readline
from glob import glob

from chunker.repo import Repo
from chunker.util import get_config_path, heal, log
from chunker.net import MetaNet


class NonExitingArgumentParser(argparse.ArgumentParser):
    def exit(self, status=0, message=None):
        raise Exception(message)


class Main(object):
    def __init__(self, args=[]):
        self.config_file_path = get_config_path("main.conf")
        try:
            self.config = json.loads(open(self.config_file_path).read())
        except Exception as e:
            log("Error loading default config: %s" % str(e))
            self.config = {}

        self.repos = []
        for filename in glob(get_config_path("*.state")):
            repo = Repo(filename)
            self.repos.append(repo)

        if args:
            print self.do(args)
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
                print self.do(cmd.split())
            except (KeyboardInterrupt, EOFError) as e:
                break
            except Exception as e:
                print {"status": "error", "message": str(e)}

    def do(self, argv):
        parser = NonExitingArgumentParser(description="a thing")
        subparsers = parser.add_subparsers()

        p_add = subparsers.add_parser("add")
        p_add.add_argument("--name")
        p_add.add_argument("chunkfile")
        p_add.add_argument("directory")
        p_add.set_defaults(func=self.cmd_add)

        p_create = subparsers.add_parser("create")
        p_create.add_argument("chunkfile")
        p_create.add_argument("directory")
        p_create.add_argument("--name")
        p_create.add_argument("--add", default=False, action="store_true")
        p_create.set_defaults(func=self.cmd_create)

        x = """
        p_heal = subparsers.add_parser("heal")
        #p_heal.add_argument("filename")
        #p_heal.add_argument("directory")
        p_heal.set_defaults(func=cmd_heal)

        p_fetch = subparsers.add_parser("fetch")
        p_fetch.set_defaults(func=cmd_fetch)

        p_stat = subparsers.add_parser("stat")
        p_stat.add_argument("filename")
        p_stat.add_argument("directory")
        p_stat.set_defaults(func=cmd_stat)
        """

        args = parser.parse_args(argv)
        return args.func(args)

    def cmd_add(self, args):
        r = Repo(
            args.chunkfile,
            args.directory,
            args.name or os.path.basename(args.directory),
        )
        r.save_state()
        self.repos.append(r)
        r.start()
        return {"status": "ok"}

    def cmd_create(self, args):
        r = Repo(args.chunkfile, args.directory, args.name or os.path.basename(args.input))
        r.save(args.output, state=False)
        if args.add:
            r.save_state()
            self.repos.append(r)
            r.start()
        return {"status": "ok"}

    def cmd_save(self, args):
        file(self.config_file_path, "w").write(json.dumps(self.config))
        return {"status": "ok"}




def cmd_heal(args, config):
    known = []
    missing = []
    for filename in glob(get_config_path("*.state")):
        r = Repo(filename)
        known.extend(r.get_known_chunks())
        missing.extend(r.get_missing_chunks())
    saved = heal(known, missing)
    return {"status": "ok", "saved": saved}


def cmd_fetch(args, config):
    repos = []
    all_known_chunks = []
    all_missing_chunks = []
    for filename in glob(get_config_path("*.state")):
        repo = Repo(filename)
        repos.append(repo)
        all_known_chunks.extend(repo.get_known_chunks())
        all_missing_chunks.extend(repo.get_missing_chunks())

    log("Repos:")
    for repo in repos:
        log(repo)

    mn = MetaNet(config)

    for chunk in set(all_known_chunks):
        mn.offer(chunk)

    for chunk in set(all_missing_chunks):
        mn.request(chunk)

    import pdb; pdb.set_trace()


def cmd_stat(args, config):
    r = Repo(args.filename, args.directory, os.path.basename(args.filename))
    chunks = r.get_known_chunks()
    seen = {}
    for chunk in chunks:
        if chunk.id not in seen:
            seen[chunk.id] = 0
        seen[chunk.id] = seen[chunk.id] + 1
    import pprint
    pprint.pprint(seen)

    saved = 0
    for id, count in seen.items():
        dupes = count - 1
        type, size, hash = id.split(":")
        saved = saved + int(size) * dupes
    print "Saved %d bytes from deduplication" % saved





def main():
    Main(sys.argv[1:])

if __name__ == "__main__":
    Main(sys.argv[1:])
