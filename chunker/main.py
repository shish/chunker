#!/usr/bin/env python

from chunker.repo import Repo
from chunker.disk import DiskWatcher
from chunker.net import NetManager

from pydispatch import dispatcher

import json
import os
from time import time


class Main(object):
    def handle_ui_alert(self, title, message):
        print "!!!", title, message

    def __init__(self):
        dispatcher.connect(self.handle_ui_alert, signal="ui:alert", sender=dispatcher.Any)
        dispatcher.connect(self.share_create, signal="share:create", sender=dispatcher.Any)
        dispatcher.connect(self.share_delete, signal="share:delete", sender=dispatcher.Any)

        self._config = {
            "repos": {},
            "peers": [],
        }

        self.load_config()

        self.net_manager = NetManager()

        _default_repo = None

        for id, repodata in self._config["repos"].items():
            _default_repo = repodata["id"]
            dispatcher.send(signal="share:create", sender=self, id=id, root=repodata["root"])

        print "'help' for help, 'quit' to quit"
        while True:
            cmd, _, args = raw_input("%s> " % _default_repo).partition(" ")
            args = args.split()
            if cmd == "quit":
                break
            elif cmd == "repo":
                if args[0] == "set":
                    _default_repo = args[1]
                elif args[0] == "create":
                    _default_repo = args[1]
                    dispatcher.send(signal="share:create", sender=self, id=args[1], root=args[2])
                elif args[0] == "delete":
                    dispatcher.send(signal="share:delete", sender=args[1], id=args[1])
                elif args[0] == "heal":
                    dispatcher.send(signal="cmd:heal", sender=_default_repo)
                else:
                    print "unknown repo subcommand"
            elif cmd == "file":
                if args[0] == "create":
                    dispatcher.send(signal="file:update", sender=_default_repo, filedata={
                        "filename": args[1],
                        "deleted": False,
                        "timestamp": int(time()),
                        "chunks": [
                            {
                                "length": 127622,
                                "hash_type": "md5",
                                "hash": "0dd18edaddf3f40818d5a4e2dfcb28c2",
                            }
                        ],
                    })
                elif args[0] == "delete":
                    dispatcher.send(signal="file:update", sender=_default_repo, filedata={
                        "filename": args[1],
                        "deleted": True,
                        "timestamp": int(time()),
                        "chunks": [],
                    })
                else:
                    print "unknown file subcommand"
            elif cmd == "save":
                self.save_config()
            else:
                print "quit - quit the app"
                print "help - print this message"

        self.save_config()

    def share_create(self, id, root):
        Repo(id, root).start()
        DiskWatcher(id, root).start()

        if id not in self._config["repos"]:
            self._config["repos"][id] = {
                "id": id,
                "root": root,
                "peers": [],
            }

    def share_delete(self, id):
        if id in self._config["repos"]:
            del self._config["repos"][id]

    def _mkconfigdir(self):
        config_dir = os.path.expanduser("~/.config/chunker")
        if not os.path.exists(config_dir):
            os.makedirs(config_dir)

    def get_config_path(self):
        return os.path.expanduser("~/.config/chunker/config.json")

    def load_config(self):
        if os.path.exists(self.get_config_path()):
            data = file(self.get_config_path()).read()
            self._config.update(json.loads(data))

    def save_config(self):
        self._mkconfigdir()
        #self._config["repos"] = [share.repo.__dict__(local=True) for share in self.shares]
        data = json.dumps(self._config, indent=4)
        file(self.get_config_path(), "w").write(data)


if __name__ == "__main__":
    Main()
