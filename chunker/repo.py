import os
import hashlib
import sys
import json
import gzip
import uuid
from mmap import mmap
from glob import glob
from datetime import datetime
from collections import deque
from pyinotify import WatchManager, ThreadedNotifier, ProcessEvent, ALL_EVENTS

#if sys.version_info < (3, 4):
#   import sha3

from chunker.util import log, get_config_path, heal, ts_round, sha256


HASH_TYPE = "sha256"


def get_chunks(fullpath, parent=None):
    """
    Split a file into chunks that are likely to be common between files

    ie, "xxxxyyzzzz" -> "xxxx", "yy", "zzzz"
        "xxxxzzzz"   -> "xxxx", "zzzz"

    TODO: actually do this. See the turbochunker directory for various attempts
    at doing this with different algorithms. For now we stick with the simple
    "1MB chunks" method, which can be changed later.

    Heuristic methods are all likely to be worse than "manual" chunking - by
    which I mean if you know all the files in advance (eg if you have a folder
    full of linux .isos) then you can write a program to look at all of the
    files at once and detect their common parts precisely.

    The implementation of the chunker can be changed fairly freely -- as long
    as one source (eg, "debian") sticks to one method, we should see benefits.
    (The odds of a linux ISO having chunks in common with a movie is fairly
    minimal)

    There is some benefit to having a standard scheme though - if two groups
    of people are sharing the same files, but they don't know about each other,
    then having the same chunks will allow them to all work together.
    """
    bite_size = 1024 * 1024
    chunks = []
    eof = False
    offset = 0
    fp = file(fullpath)
    while not eof:
        data = fp.read(bite_size)
        if not data:
            break
        if len(data) < bite_size:
            eof = True
        if parent:
            chunks.append(Chunk(parent, offset, len(data), HASH_TYPE, hashlib.new(HASH_TYPE, data).hexdigest(), True))
        else:
            chunks.append({
                "hash_type": HASH_TYPE,
                "hash": hashlib.new(HASH_TYPE, data).hexdigest(),
                "offset": offset,
                "length": len(data),
                "saved": True,
            })
        offset = offset + len(data)
    return chunks


class Chunk(object):
    def __init__(self, file, offset, length, hash_type, hash, saved=False):
        self.file = file
        self.offset = offset
        self.length = length
        self.hash_type = hash_type
        self.hash = hash
        self.saved = saved

    def to_struct(self, state=False):
        data = {
                "hash_type": self.hash_type,
                "length": self.length,
                "hash": self.hash,
        }
        if state:
            data["saved"] = self.saved
        return data

    def __repr__(self):
        return self.id

    @property
    def id(self):
        return "%s:%s:%s" % (self.hash_type, self.length, self.hash)

    def __cmp__(self, other):
        return cmp(self.id, other.id)

    def validate(self):
        self.saved = hashlib.new(self.hash_type, self.get_data()).hexdigest() == self.hash

    def get_data(self):
        #self.log("Reading chunk")
        try:
            fp = file(self.file.fullpath)
            fp.seek(self.offset)
            return fp.read(self.length)
        except IOError:
            return ""

    def save_data(self, data):
        #self.log("Saving chunk")
        if os.path.exists(self.file.fullpath):
            st = os.stat(self.file.fullpath)
            atime = st.st_atime
            mtime = st.st_mtime
            f = open(self.file.fullpath,'r+b')
        else:
            atime = 0
            mtime = 0
            f = open(self.file.fullpath,'wb')

        f.seek(self.offset)
        f.write(data)
        f.close()
        self.saved = True

        if self.file.is_complete():
            # set file timestamp to new metadata timestamp
            self.file.log("File complete, updating timestamp")
            os.utime(self.file.fullpath, (atime, self.file.timestamp))
        else:
            # set file timestamp or old metadata timestamp
            os.utime(self.file.fullpath, (atime, mtime))

    def log(self, msg):
        self.file.log("[%s:%s] %s" % (self.offset, self.length, msg))


class FileVersion(object):
    def __init__(self):
        self.timestamp = 0
        self.deleted = False
        self.chunks = []
        self.username = ""
        self.hostname = ""

    def __cmp__(self, other):
        return cmp(self.timestamp, other.timestamp)

    def is_complete(self):
        return self.get_missing_chunks() == []

    def get_missing_chunks(self):
        l = []
        for chunk in self.chunks:
            if not chunk.saved:
                l.append(chunk)
        return l

    def get_known_chunks(self):
        l = []
        for chunk in self.chunks:
            if chunk.saved:
                l.append(chunk)
        return l

    @staticmethod
    def from_struct(file, versionData):
        version = FileVersion()
        version.deleted = versionData.get("deleted", False)
        version.timestamp = versionData.get("timestamp", 0)
        version.username = versionData.get("username", "Origin User")
        version.hostname = versionData.get("hostname", "Origin Host")

        if versionData.get("chunks") is not None:
            offset = 0
            for chunkData in versionData["chunks"]:
                version.chunks.append(
                    Chunk(file, offset, chunkData["length"], chunkData["hash_type"], chunkData["hash"])
                )
                offset = offset + chunkData["length"]
            for chunk in version.chunks:
                chunk.validate()
        elif os.path.exists(file.fullpath):
            version.chunks = get_chunks(file.fullpath, version)
        return version

    def to_struct(self, state=False, history=False):
        data = {
            "chunks": [chunk.to_struct(state=state) for chunk in self.chunks],
            "timestamp": self.timestamp,
            "deleted": self.deleted,
        }
        if history:
            data.update({
                "username": self.username,
                "hostname": self.hostname,
            })
        return data



class File(object):
    def __init__(self, repo, filename, chunks=None):
        self.repo = repo
        self.filename = filename
        self.fullpath = os.path.join(self.repo.root, self.filename)
        self.versions = []

        if not os.path.abspath(self.fullpath).startswith(os.path.abspath(self.repo.root)):
            raise Exception("Tried to create a file outside the repository: %s" % self.filename)

    @staticmethod
    def from_struct(repo, filename, data):
        assert(isinstance(repo, Repo))
        assert(isinstance(filename, basestring))
        assert(isinstance(data, dict))

        file = File(repo, filename, [])
        for versionData in data["versions"]:
            version = FileVersion.from_struct(file, versionData)
            file.versions.append(version)
        return file

    def to_struct(self, state=False, history=False):
        data = {}
        if history:
            data["versions"] = [version.to_struct(state=state, history=history) for version in self.versions]
        else:
            data["versions"] = [self.current_version().to_struct(state=state, history=history)]
        return data

    def log(self, msg):
        self.repo.log("[%s] %s" % (self.filename, msg))

    # proxy version-specific attributes to the latest version
    def current_version(self):
        return self.versions[-1]

    def get_missing_chunks(self):
        return self.current_version().get_missing_chunks()

    def get_known_chunks(self):
        return self.current_version().get_known_chunks()

    @property
    def timestamp(self):
        return self.current_version().timestamp

    @property
    def deleted(self):
        return self.current_version().deleted

    def is_complete(self):
        return self.current_version().is_complete()


class Repo(object, ProcessEvent):
    def __init__(self, filename, root=None, name=None):
        """
        Load a repository metadata structure

        filename:
          the name of a .chunker file to load, containing
          either full state, or a useful subset

        root:
          where the repository should live on disk
          (if not included in the state file)

        name:
          a unique (to this chunker process) name for this
          repository (if not included in the state file)
        """
        self.filename = filename
        if os.path.exists(self.filename):
            try:
                data = gzip.open(self.filename).read()
            except:
                data = open(self.filename).read()
            struct = json.loads(data)
        else:
            struct = {}

        self.name = name or struct.get("name") or os.path.basename(filename)
        self.type = struct.get("type", "static")  # static / share
        self.uuid = struct.get("uuid", sha256(uuid.uuid4()))
        self.key = struct.get("key", None)        # for encrypting / decrypting chunks
        self.peers = struct.get("peers", [])
        self.root = root or struct.get("root")
        self.files = dict([
            (filename, File.from_struct(self, filename, data))
            for filename, data
            in struct.get("files", {}).items()
        ])

        # if we're creating a static chunkfile, or connecting to a share where
        # we can add files, then add our local files to the chunkfile
        if (self.type == "static" and not struct) or (self.type == "share"):
            self.__add_local_files()

        if self.type == "share":
            watcher = WatchManager()
            watcher.add_watch(self.root, ALL_EVENTS, rec=True, auto_add=True)
            self.notifier = ThreadedNotifier(watcher, self)
            self.notifier.daemon = True
        else:
            self.notifier = None

    def to_struct(self, state=False):
        """
        Serialise the repository into a JSON-compatible dictionary
        """
        data = {
            "name": self.name,
            "type": self.type,
            "secret": self.secret,
            "peers": self.peers,
            "files": dict([
                (filename, file.to_struct(state=state))
                for filename, file
                in self.files.items()
            ])
        }
        if state:
            data.update({
                "root": self.root
            })
        return data

    def __repr__(self):
        return "Repo(%r, %r, %r)" % (self.filename, self.root, self.name)

    def save_state(self):
        """
        Save the repository state to the default state location
        (eg ~/.config/chunker/<uuid>.state on unix)
        """
        self.save(get_config_path(self.uuid+".state"), state=True, compress=True)

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
        if not filename:
            filename = self.filename

        struct = self.to_struct(state=state)

        if compress:
            fp = gzip.open(filename, "w")
            data = json.dumps(struct)
        else:
            fp = open(filename, "w")
            data = json.dumps(struct, indent=4)

        fp.write(data)
        fp.close()

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
        # this could be much more efficient -
        # sort the lists, then go through each list once linearly
        if known_chunks is None:
            known_chunks = self.get_known_chunks()
        if missing_chunks is None:
            missing_chunks = self.get_missing_chunks()

        heal(known_chunks, missing_chunks)

    def log(self, msg):
        log("[%10.10s] %s" % (self.name, msg))

    def __add_local_files(self):
        base = os.path.abspath(self.root)
        for filename in glob(self.root+"/*"):
            path = os.path.abspath(filename)
            relpath = path[len(base)+1:]
            if (
                relpath not in self.files or  # file on disk that we haven't seen before
                ts_round(os.stat(path).st_mtime) > self.files[relpath].timestamp  # update for a file we know about
            ):
                self.update(relpath, {
                    "versions": [{
                        "timestamp": ts_round(os.stat(path).st_mtime),
                        "chunks": None,
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
    # File system monitoring
    ###################################################################

    def start(self):
        """
        Start monitoring for file changes
        """
        if self.type == "share":
            self.log("Watching %s for file changes" % self.root)
            self.notifier.start()
            self.__add_local_files()

        self.self_heal()

    def stop(self):
        """
        Stop monitoring for file changes
        """
        if self.notifier:
            self.log("No longer watching %s for file changes" % self.root)
            self.notifier.stop()

    def __relpath(self, path):
        base = os.path.abspath(self.root)
        path = os.path.abspath(path)
        return path[len(base)+1:]

    def process_IN_CREATE(self, event):
        if os.path.isdir(event.pathname):
            return
        path = self.__relpath(event.pathname)
        self.update(path, filedata={
            "timestamp": int(os.stat(event.pathname).st_mtime),
            "chunks": get_chunks(event.pathname),
        })

#    def process_IN_MODIFY(self, event):
#        if os.path.isdir(event.pathname):
#            return
#        path = self.__relpath(event.pathname)
#        self.update(path, filedata={
#            "deleted": False,
#            "timestamp": int(time()),
#            "chunks": get_chunks(event.pathname),
#        })

    def process_IN_DELETE(self, event):
        if os.path.isdir(event.pathname):
            return
        path = self.__relpath(event.pathname)
        self.update(path, filedata={
            "deleted": True,
            "timestamp": int(time()),
            "chunks": [],
        })

