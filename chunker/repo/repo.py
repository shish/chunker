from pyinotify import WatchManager, ThreadedNotifier, ProcessEvent, ALL_EVENTS
from Crypto.Cipher import AES
from glob import glob
from select import select
from threading import Thread
from datetime import datetime
from time import time, sleep
import json
import gzip
import os
import uuid
import logging


from chunker.util import get_config_path, heal, ts_round, sha256, config
from .file import File


log = logging.getLogger(__name__)


class Repo(ProcessEvent):
    def __init__(self, filename=None, config={}, **kwargs):
        """
        Load a repository metadata structure

        filename:
          the name of a .chunker file to load, containing
          either full state, or a useful subset

        **kwargs:
          a basic .chunker data structure

        config:
          optional dictionary of extra info. Keys:
            username - for change log
            hostname - for change log
        """
        self.notifier = None

        if not filename and not kwargs:
            raise Exception("Repo has no initialisation data")

        struct = {}
        if filename and os.path.exists(filename):
            try:
                data = gzip.open(filename).read()
            except:
                data = open(filename).read()
            struct.update(json.loads(data))
        struct.update(kwargs)

        self.config = config

        self.name = struct.get("name") or os.path.basename(struct.get("root")) or os.path.splitext(os.path.basename(filename or ""))[0]
        self.root = struct.get("root") or os.path.join(os.path.expanduser("~/Downloads"), self.name)
        self.type = struct.get("type", "share")  # static / share
        self.uuid = struct.get("uuid", sha256(uuid.uuid4()))
        self.key = struct.get("key", None)       # for encrypting / decrypting chunks
        self.peers = struct.get("peers", [])
        self.files = dict([
            (filename, File.from_struct(self, filename, data))
            for filename, data
            in struct.get("files", {}).items()
        ])

        # if we're creating a new static chunkfile, then add our local files to the chunkfile
        # should this be in start()?
        if (self.type == "static" and not self.files):
            self.__add_local_files()

    def to_struct(self, state=False):
        """
        Serialise the repository into a JSON-compatible dictionary
        """
        data = {
            "name": self.name,
            "type": self.type,
            "uuid": self.uuid,
            "key": self.key,
            "files": dict([
                (filename, file.to_struct(state=state))
                for filename, file
                in self.files.items()
            ])
        }
        if state:
            data.update({
                "peers": [
                    peer.to_struct(state=state)
                    for peer
                    in self.peers
                ],
                "root": self.root,
            })
        return data

    def __repr__(self):
        return "Repo(%r, %r, %r, %r)" % (self.type, self.uuid, self.root, self.name)

    def save_state(self):
        """
        Save the repository state to the default state location
        (eg ~/.config/chunker/<uuid>.state on unix)
        """
        self.save(get_config_path(self.uuid + ".state"), state=True, compress=True)

    def remove_state(self):
        p = get_config_path(self.uuid + ".state")
        if os.path.exists(p):
            os.unlink(p)

    def save(self, filename=None, state=False, compress=False):
        """
        Export the repository state (ie, write Repo.to_struct() to a JSON file)

        filename:
          where to save the state to

        state:
          whether to save active state, eg which chunks are currently downloaded
            True -> useful for an app to exit and re-open on the same PC later
            False -> useful for exporting the minimal amount of info to get a
                     new node to join the swarm

        compress:
          whether or not to run the data through gzip (disabling this can make
          debugging easier)
        """
        struct = self.to_struct(state=state)

        if compress:
            fp = gzip.open(filename, "w")
            data = json.dumps(struct)
        else:
            fp = open(filename, "w")
            data = json.dumps(struct, indent=4)

        fp.write(data)
        fp.close()

    def log(self, msg):
        log.info("[%s] %s" % (self.name, msg))

    ###################################################################
    # Metadata
    ###################################################################

    def __add_local_files(self):
        for dirpath, dirnames, filenames in os.walk(self.root):
            for filename in filenames:
                path = os.path.join(dirpath, filename)
                relpath = self.__relpath(path)
                # look for
                # - files that we haven't seen before
                # - files with newer timestamps than our latest known version
                #
                # note that if a file has new content, but the timestamp is
                # unchanged since we last saw it, we won't add a new version,
                # but rather treat the current version as corrupt
                if (
                    relpath not in self.files or
                    ts_round(os.stat(path).st_mtime) > self.files[relpath].timestamp
                ):
                    self.update(relpath, {
                        "versions": [{
                            "timestamp": ts_round(os.stat(path).st_mtime),
                            "chunks": None,
                        }]
                    })

        for file in self.files.values():
            # "not supposed to be deleted, but it is" -> it has been
            # deleted while we weren't looking.
            if not file.deleted and not os.path.exists(file.fullpath):
                # We don't know when it was deleted, so add the deletion
                # tag as just after the last modification (ie, mark that
                # version as deleted, but any newer remote version takes
                # precedence)
                self.update(file.filename, {
                    "versions": [{
                        "timestamp": ts_round(file.timestamp + 1),
                        "chunks": [],
                        "deleted": True,
                    }]
                })

    def update(self, filename, filedata):
        """
        Update the repository with new metadata for a named file
        """
        file = File.from_struct(self, filename, filedata)

        if file.filename not in self.files:
            self.files[file.filename] = file
        else:
            self.files[file.filename].versions.extend(file.versions)

        self.files[file.filename].versions.sort()

        if file.deleted:
            file.log("deleted")
            if os.path.exists(file.fullpath):
                os.unlink(file.fullpath)
        else:
            if os.path.exists(file.fullpath):
                file.log("updated")
            else:
                with open(file.fullpath, "a"):
                    if file.is_complete():
                        # mark as finished already
                        os.utime(file.fullpath, (file.timestamp, file.timestamp))
                    else:
                        # mark as incomplete
                        os.utime(file.fullpath, (0, 0))
                file.log("created")

        self.save_state()

    ###################################################################
    # Networking
    ###################################################################

    def add_peer(self, peer):
        if peer not in self.peers:
            self.log("Found new peer: %r" % (peer, ))
            self.peers.append(peer)

    ###################################################################
    # Chunks
    ###################################################################

    def get_missing_chunks(self):
        """
        Get a list of missing chunks
        """
        l = []
        for file in self.files.values():
            l.extend(file.get_missing_chunks())
        return l

    def get_known_chunks(self):
        """
        Get a list of known chunks
        """
        l = []
        for file in self.files.values():
            l.extend(file.get_known_chunks())
        return l

    def add_chunk(self, chunk_id, data):
        """
        Notify the repository that a new chunk is available
        (probably freshly downloaded from the network)
        """
        self.log("Trying to insert chunk %s into files" % chunk_id)
        for chunk in self.missing_chunks:
            if chunk.id == chunk_id:
                chunk.save_data(data)

    def self_heal(self, known_chunks=None, missing_chunks=None):
        """
        Try to use known chunks to fill in gaps
        """
        if known_chunks is None:
            known_chunks = self.get_known_chunks()
        if missing_chunks is None:
            missing_chunks = self.get_missing_chunks()

        heal(known_chunks, missing_chunks)

    ###################################################################
    # Crypto
    ###################################################################

    def encrypt(self, data):
        if self.key:
            c = AES.new(self.key)
            data = c.encrypt(data)
        return data

    def decrypt(self, data):
        if self.key:
            c = AES.new(self.key)
            data = c.decrypt(data)
        return data

    ###################################################################
    # File system monitoring
    ###################################################################

    def start(self):
        """
        Start monitoring for file changes
        """
        if self.type == "share":
            self.log("Checking for files updated while we were offline")
            self.__add_local_files()
            self.log("Watching %s for file changes" % self.root)
            watcher = WatchManager()
            watcher.add_watch(self.root, ALL_EVENTS, rec=True, auto_add=True)
            self.notifier = ThreadedNotifier(watcher, self)
            self.notifier.daemon = True
            self.notifier.start()
        else:
            self.log("Not watching %s for file changes" % self.root)

        # self.self_heal()

        def netcomms():
            while True:
                # select()'ing three empty lists is an error on windows
                if not self.peers:
                    sleep(5)
                    continue

                rs, ws, xs = select(self.peers, self.peers, [], 0)

                for r in rs:
                    packet = r.recv()
                    r.last_pong = time()
                    print "Received", packet

                for w in ws:
                    if w.last_ping < time() - 60 and w.last_pong < time() - 60:
                        data = json.dumps({"cmd": "get-status", "since": w.last_update})
                        print "Sending", data
                        w.send(data)
                        w.last_ping = time()

                for peer in self.peers:
                    if peer.last_pong < time() - 300:
                        log.info("Peer no longer reachable - %r" % peer)

                # if there was nothing to do, sleep for a bit
                # (if there was something to do, immediately go back for more)
                if not rs:
                    sleep(1)

        nc = Thread(target=netcomms, name="NetComms[%s]" % self.name)
        nc.daemon = True
        nc.start()

    def stop(self):
        """
        Stop monitoring for file changes
        """
        if self.notifier:
            self.log("No longer watching %s for file changes" % self.root)
            self.notifier.stop()
            self.notifier = None

    def __relpath(self, path):
        base = os.path.abspath(self.root)
        path = os.path.abspath(path)
        return path[len(base)+1:]

    def process_IN_CREATE(self, event):
        if os.path.isdir(event.pathname):
            return
        path = self.__relpath(event.pathname)
        self.update(path, filedata={
            "versions": [{
                "timestamp": int(os.stat(event.pathname).st_mtime),
                "chunks": None,
                "username": self.config.get("username"),
                "hostname": self.config.get("hostname"),
            }]
        })

#    def process_IN_MODIFY(self, event):
#        if os.path.isdir(event.pathname):
#            return
#        path = self.__relpath(event.pathname)
#        self.update(path, filedata={
#            "versions": [{
#                "deleted": False,
#                "timestamp": int(os.stat(event.pathname).st_mtime),
#                "chunks": None,
#                "username": self.config.get("username"),
#                "hostname": self.config.get("hostname"),
#            }]
#        })

    def process_IN_DELETE(self, event):
        if os.path.isdir(event.pathname):
            return
        path = self.__relpath(event.pathname)
        self.update(path, filedata={
            "versions": [{
                "deleted": True,
                "timestamp": int(time()),
                "chunks": [],
                "username": self.config.get("username"),
                "hostname": self.config.get("hostname"),
            }]
        })
