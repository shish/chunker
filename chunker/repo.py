import os
import hashlib
import sys
import json
from mmap import mmap
from glob import glob
from datetime import datetime
from collections import deque
from pydispatch import dispatcher

if sys.version_info < (3, 4):
   import sha3

from chunker.util import log, get_config_path, heal, ts_round


HASH_TYPE = "sha3_256"


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

    def validate(self):
        self.saved = hashlib.new(HASH_TYPE, self.get_data()).hexdigest() == self.hash

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
            dispatcher.send(
                signal="ui:alert",
                sender=self.file.repo.name,
                title="Download Complete",
                message=self.file.filename
            )
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


class Repo(object):
    def __init__(self, filename, root=None, name=None):
        self.filename = filename
        if os.path.exists(self.filename):
            struct = json.loads(file(self.filename).read())
        else:
            struct = {}

        self.name = name or struct.get("name") or os.path.basename(filename)
        self.type = struct.get("type", "static")  # static / share
        self.secret = struct.get("secret", None)  # only for share
        self.peers = struct.get("peers", [])      # only for share?
        self.root = root or struct.get("root")
        self.files = dict([
            (filename, File.from_struct(self, filename, data))
            for filename, data
            in struct.get("files", {}).items()
        ])

        # if we're creating a static chunkfile, or connecting to a share where
        # we can add files, then add our local files to the chunkfile
        if (self.type == "static" and not struct) or (self.type == "share"):
            dispatcher.connect(self.update, signal="file:update", sender=self.name)
            self.add_local_files()

    def to_struct(self, state=False):
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
        self.save(get_config_path(hashlib.new(HASH_TYPE, self.name).hexdigest()+".state"), True)

    def save(self, filename=None, state=False):
        if not filename:
            filename = self.filename
        file(filename, "w").write(json.dumps(self.to_struct(state=state), indent=4))

    def start(self):
        dispatcher.connect(self.chunk_found, signal="chunk:found", sender=dispatcher.Any)
        dispatcher.connect(self.self_heal, signal="cmd:heal", sender=self.name)
        dispatcher.connect(self.update, signal="file:update", sender=self.name)

        if self.type == "share":
            self.add_local_files()

        self.self_heal()

    def add_local_files(self):
        base = os.path.abspath(self.root)
        for filename in glob(self.root+"/*"):
            path = os.path.abspath(filename)
            relpath = path[len(base)+1:]
            if (
                relpath not in self.files or  # file on disk that we haven't seen before
                ts_round(os.stat(path).st_mtime) > self.files[relpath].timestamp  # update for a file we know about
            ):
                dispatcher.send(signal="file:update", sender=self.name, filename=relpath, filedata={
                    "versions": [{
                        "deleted": False,
                        "timestamp": ts_round(os.stat(path).st_mtime),
                        "chunks": None,
                    }]
                })

    def get_missing_chunks(self):
        l = []
        for file in self.files.values():
            l.extend(file.get_missing_chunks())
        return l

    def get_known_chunks(self):
        l = []
        for file in self.files.values():
            l.extend(file.get_known_chunks())
        return l

    def chunk_found(self, chunk_id, data):
        self.log("Trying to insert chunk %s into files" % chunk_id)
        for chunk in self.missing_chunks:
            if chunk.id == chunk_id:
                chunk.save_data(data)

    def self_heal(self, known_chunks=None, missing_chunks=None):
        # this could be much more efficient -
        # sort the lists, then go through each list once linearly
        if known_chunks is None:
            known_chunks = self.get_known_chunks()
        if missing_chunks is None:
            missing_chunks = self.get_missing_chunks()

        heal(known_chunks, missing_chunks)

    def log(self, msg):
        log("[%10.10s] %s" % (self.name, msg))

    def update(self, filename, filedata):
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


if __name__ == "__main__":
    file("./test-repo.chunker", "w").write(json.dumps(
        {
            "name": "My Test Repo",
            "type": "static",
            "files": {
                "hello1.txt": {
                    "chunks": [
                        {
                            "hash_type": "md5",
                            "hash": "5a8dd3ad0756a93ded72b823b19dd877",
                            "length": 6,
                        }
                    ],
                    "timestamp": 0,
                    "deleted": False,
                },
                "hello2.txt": {
                    "chunks": [
                        {
                            "hash_type": "md5",
                            "hash": "5a8dd3ad0756a93ded72b823b19dd877",
                            "length": 6,
                        }
                    ],
                    "timestamp": 0,
                    "deleted": False,
                }
            }
        }
    , indent=4))
    #os.makedirs("test-repo")
    file("test-repo/hello1.txt", "w").write("hello!")
    r = Repo("./test-repo.chunker", "./test-repo")
    print r.get_known_chunks()
    print r.get_missing_chunks()
    r.self_heal()
    print r.get_known_chunks()
    print r.get_missing_chunks()
    r.save_state()

