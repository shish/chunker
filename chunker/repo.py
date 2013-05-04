import os
import hashlib
import json
from glob import glob
from datetime import datetime
from pydispatch import dispatcher

from chunker.util import log, get_config_path, heal, ts_round


HASH_TYPE = "md5"
HASH = hashlib.md5


def get_chunks(fullpath, parent=None):
    """
    Split a file into chunks that are likely to be common between files

    ie, "xxxxyyzzzz" -> "xxxx", "yy", "zzzz"
        "xxxxzzzz"   -> "xxxx", "zzzz"

    TODO: actually do this (currently we just split into 1MB parts)
          gzip --rsyncable seems to have a pretty sensible approach
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
            chunks.append(Chunk(parent, offset, len(data), HASH_TYPE, HASH(data).hexdigest(), True))
        else:
            chunks.append({
                "hash_type": HASH_TYPE,
                "hash": HASH(data).hexdigest(),
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

    def __dict__(self, state=False):
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
        self.saved = HASH(self.get_data()).hexdigest() == self.hash

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


class File(object):
    def __init__(self, repo, filename, chunks=None):
        self.repo = repo
        self.filename = filename
        self.fullpath = os.path.join(self.repo.root, self.filename)
        self.chunks = chunks
        self.deleted = False

        if not os.path.abspath(self.fullpath).startswith(os.path.abspath(self.repo.root)):
            raise Exception("Tried to create a file outside the repository: %s" % self.filename)

        # this is a new file locally
        if chunks is None and os.path.exists(self.fullpath):
            self.chunks = get_chunks(self.fullpath, self)
            self.timestamp = ts_round(os.stat(self.fullpath).st_mtime)

    @staticmethod
    def from_struct(repo, filename, data):
        file = File(repo, filename, [])
        file.deleted = data.get("deleted", False)
        file.timestamp = data.get("timestamp", 0)

        if data.get("chunks"):
            offset = 0
            for chunkdata in data["chunks"]:
                file.chunks.append(
                    Chunk(file, offset, chunkdata["length"], chunkdata["hash_type"], chunkdata["hash"])
                )
                offset = offset + chunkdata["length"]
            for chunk in file.chunks:
                chunk.validate()
        elif os.path.exists(file.fullpath):
            file.chunks = get_chunks(file.fullpath, file)
        return file

    @property
    def missing_chunks(self):
        l = []
        for chunk in self.chunks:
            if not chunk.saved:
                l.append(chunk)
        return l

    @property
    def known_chunks(self):
        l = []
        for chunk in self.chunks:
            if chunk.saved:
                l.append(chunk)
        return l

    def is_complete(self):
        return self.missing_chunks == []

    def __dict__(self, state=False):
        return {
            "chunks": [chunk.__dict__(state=state) for chunk in self.chunks],
            "timestamp": self.timestamp,
            "deleted": self.deleted,
        }

    def log(self, msg):
        self.repo.log("[%s] %s" % (self.filename, msg))


class Repo(object):
    def __init__(self, filename, root=None, name=None):
        self.filename = filename
        if os.path.exists(self.filename):
            struct = json.loads(file(self.filename).read())
        else:
            struct = {}

        self.name = name or struct.get("name") or os.path.basname(filename)
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

    def __dict__(self, state=False):
        data = {
            "name": self.name,
            "type": self.type,
            "secret": self.secret,
            "peers": self.peers,
            "files": dict([
                (filename, file.__dict__(state=state))
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
        self.save(get_config_path(HASH(self.name).hexdigest()+".state"), True)

    def save(self, filename=None, state=False):
        if not filename:
            filename = self.filename
        file(filename, "w").write(json.dumps(self.__dict__(state=state), indent=4))

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
                dispatcher.send(signal="file:update", sender=self.name, filedata={
                    "filename": relpath,
                    "deleted": False,
                    "timestamp": ts_round(os.stat(path).st_mtime),
                    "chunks": None,
                })

    def get_missing_chunks(self):
        l = []
        for file in self.files.values():
            l.extend(file.missing_chunks)
        return l

    def get_known_chunks(self):
        l = []
        for file in self.files.values():
            l.extend(file.known_chunks)
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


    def update(self, filedata):
        file = File.from_struct(self, filedata["filename"], filedata)

        if (
            # never seen this file before
            (file.filename not in self.files) or
            # new version of file we've seen before
            (file.timestamp > self.files[file.filename].timestamp)
        ):
            self.files[file.filename] = file

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

        else:
            # old version of file we've seen before
            pass


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

