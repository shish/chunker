import sys
import json
import os
import readline
import platform
from glob import glob

from chunker.repo import Repo
from chunker.util import get_config_path, heal, log
from chunker.net import MetaNet


class Core(object):
    def __init__(self):
        self.config_file_path = get_config_path("main.conf")
        self.config = {
            "username": "TODO",
            "hostname": platform.node().split(".")[0],
        }
        try:
            self.config.update(json.loads(open(self.config_file_path).read()))
        except Exception as e:
            log("Error loading default config: %s" % str(e))

        self.mn = None

        self.repos = {}
        for filename in glob(get_config_path("*.state")):
            repo = Repo(filename, config=self.config)
            self.repos[repo.uuid] = repo

    def start(self):
        for r in self.repos.values():
            r.start()

    def stop(self):
        for r in self.repos.values():
            r.stop()

    def _init_parser(self, pclass):
        self.parser = pclass(description="a thing")
        subparsers = self.parser.add_subparsers()

        p_create = subparsers.add_parser("create")
        p_create.add_argument("--chunkfile", required=True)
        p_create.add_argument("--directory", required=True)
        p_create.add_argument("--name")
        p_create.add_argument("--key")
        p_create.add_argument("--type", default="static")
        p_create.add_argument("--add", default=False, action="store_true")
        p_create.set_defaults(func=self.cmd_create)

        p_add = subparsers.add_parser("add")
        p_add.add_argument("--chunkfile", required=True)
        p_add.add_argument("--directory")
        p_add.add_argument("--name")
        p_add.add_argument("--key")
        p_add.set_defaults(func=self.cmd_add)

        p_remove = subparsers.add_parser("remove")
        p_remove.add_argument("--uuid")
        p_remove.set_defaults(func=self.cmd_remove)

        p_heal = subparsers.add_parser("heal")
        p_heal.set_defaults(func=self.cmd_heal)

        p_fetch = subparsers.add_parser("fetch")
        p_fetch.set_defaults(func=self.cmd_fetch)

        p_list = subparsers.add_parser("list")
        p_list.set_defaults(func=self.cmd_list)

        p_state = subparsers.add_parser("state")
        p_state.set_defaults(func=self.cmd_state)

        p_quit = subparsers.add_parser("quit")
        p_quit.set_defaults(func=self.cmd_quit)

    def cmd_create(self, args):
        r = Repo(type=args.type, root=args.directory, name=args.name, key=args.key, config=self.config)
        if args.chunkfile:
            r.save(args.chunkfile, state=False)
        if args.add:
            r.save_state()
            self.repos[r.uuid] = r
            r.start()
        return {"status": "ok"}

    def cmd_add(self, args):
        chunkfile = os.path.abspath(args.chunkfile)
        if not os.path.exists(chunkfile):
            raise Exception("Chunkfile %s does not exist" % chunkfile)

        r = Repo(filename=chunkfile, root=args.directory, name=args.name, key=args.key, config=self.config)
        r.save_state()
        self.repos[r.uuid] = r
        r.start()
        return {"status": "ok"}

    def cmd_remove(self, args):
        if args.uuid in self.repos:
            repo = self.repos[args.uuid]
            log("Removing %s" % repo.name)
            repo.stop()
            repo.remove_state()
            del self.repos[args.uuid]
            return {"status": "ok", "message": "Removed %s" % repo.name}
        else:
            return {"status": "error", "message": "Can't find that repo"}

    def cmd_heal(self, args):
        known = []
        missing = []
        for r in self.repos.values():
            known.extend(r.get_known_chunks())
            missing.extend(r.get_missing_chunks())
        saved = heal(known, missing)
        return {"status": "ok", "saved": saved}

    def cmd_save(self, args):
        file(self.config_file_path, "w").write(json.dumps(self.config))
        return {"status": "ok"}

    def cmd_fetch(self, args):
        all_known_chunks = []
        all_missing_chunks = []
        for repo in self.repos.values():
            all_known_chunks.extend(repo.get_known_chunks())
            all_missing_chunks.extend(repo.get_missing_chunks())

        if not self.mn:
            self.mn = MetaNet(config)

        for chunk in set(all_known_chunks):
            self.mn.offer(chunk)

        for chunk in set(all_missing_chunks):
            self.mn.request(chunk)

        import pdb; pdb.set_trace()

    def cmd_list(self, args):
        fmt = "%-15s %-7s %-s"
        print fmt % ("name", "type", "files")
        print fmt % ("~~~~", "~~~~", "~~~~~")
        for repo in self.repos.values():
            files = "%d/%d" % (len(repo.files), len([f for f in repo.files.values() if f.is_complete()]))
            print fmt % (repo.name, repo.type, files)
        return {"status": "ok"}

    def cmd_state(self, args):
        return {
            "status": "ok",
            "repos": dict([(name, repo.to_struct(state=True)) for name, repo in self.repos.items()]),
        }

    def cmd_quit(self, args):
        raise EOFError()

