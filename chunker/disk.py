from pyinotify import WatchManager, ThreadedNotifier, ProcessEvent, ALL_EVENTS
from pydispatch import dispatcher

import os
from time import time

import chunker.repo as repo
from chunker.util import log


class EventHandler(ProcessEvent):
    def my_init(self, id, root):
        self.id = id
        self.root = root

    def _relpath(self, path):
        base = os.path.abspath(self.root)
        path = os.path.abspath(path)
        return path[len(base)+1:]

    def process_IN_CREATE(self, event):
        if os.path.isdir(event.pathname):
            return
        path = self._relpath(event.pathname)

        dispatcher.send(signal="file:update", sender=self.id, filedata={
            "filename": path,
            "deleted": False,
            "timestamp": int(os.stat(event.pathname).st_mtime),
            "chunks": repo.get_chunks(event.pathname),
        })

#    def process_IN_MODIFY(self, event):
#        if os.path.isdir(event.pathname):
#            return
#        path = self._relpath(event.pathname)
#        dispatcher.send(signal="file:update", sender=self.id, filedata={
#            "filename": path,
#            "deleted": False,
#            "timestamp": int(time()),
#            "chunks": repo.get_chunks(event.pathname),
#        })

    def process_IN_DELETE(self, event):
        if os.path.isdir(event.pathname):
            return
        path = self._relpath(event.pathname)

        dispatcher.send(signal="file:update", sender=self.id, filedata={
            "filename": path,
            "deleted": True,
            "timestamp": int(time()),
            "chunks": [],
        })


class DiskWatcher(object):
    def __init__(self, id, root):
        self.id = id
        self.root = root

        self.watcher = WatchManager()
        self.handler = EventHandler(id=id, root=root)
        self.notifier = ThreadedNotifier(self.watcher, self.handler)
        self.notifier.daemon = True

    def start(self):
        self.log("Watching %s for file changes" % self.root)
        self.notifier.start()
        dispatcher.connect(self.stop, signal="share:delete", sender=self.id)
        self.watcher.add_watch(self.root, ALL_EVENTS, rec=True, auto_add=True)

    def stop(self):
        self.log("No longer watching %s for file changes" % self.root)
        self.notifier.stop()

    def log(self, msg):
        log("[%s] %s" % (self.id, msg))

