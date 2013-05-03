import os
import hashlib
from glob import glob
from datetime import datetime
from pydispatch import dispatcher

from chunker.util import log


HASH_TYPE = "md5"
HASH = hashlib.md5

def get_chunks(fullpath, parent=None):
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

    def __dict__(self):
        return {
                "hash_type": self.hash_type,
                "length": self.length,
                "hash": self.hash,
        }

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
            dispatcher.send(signal="ui:alert", sender=self.file.repo.id, title="Download Complete", message=self.file.filename)
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

        # this is a new file locally
        if chunks is None and os.path.exists(self.fullpath):
            self.chunks = get_chunks(self.fullpath, self)
            self.timestamp = int(os.stat(self.fullpath).st_mtime)

    @staticmethod
    def from_struct(repo, data):
        file = File(repo, data["filename"], [])
        file.deleted = data.get("deleted", False)
        file.timestamp = data["timestamp"]

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

    def __dict__(self):
        return {
            "filename": self.filename,
            "chunks": [chunk.__dict__() for chunk in self.chunks],
            "timestamp": self.timestamp,
            "deleted": self.deleted,
        }

    def log(self, msg):
        self.repo.log("[%s] %s" % (self.filename, msg))


class Repo(object):
    def __init__(self, id=None, root=None, struct=None):
        self.id = id
        self.files = {}
        self.root = root

        if struct:
            self.id = struct["id"]
            self.root = struct["root"]
            self.files = dict([(filename, File.from_struct(self, data)) for filename, data in struct.get("files", {}).items()])

    def start(self):
        if self.root:
            dispatcher.connect(self.file_update, signal="file:update", sender=self.id)
            dispatcher.connect(self.chunk_found, signal="chunk:found", sender=self.id)
            dispatcher.connect(self.self_heal, signal="cmd:heal", sender=self.id)

            base = os.path.abspath(self.root)
            for filename in glob(self.root+"/*"):
                path = os.path.abspath(filename)
                relpath = path[len(base)+1:]
                if (
                    relpath not in self.files or  # file on disk that we haven't seen before
                    int(os.stat(path).st_mtime) > self.files[relpath].timestamp  # update for a file we know about
                ):
                    dispatcher.send(signal="file:created", sender=self.id, filedata={
                        "filename": relpath,
                        "deleted": False,
                        "timestamp": int(os.stat(path).st_mtime),
                        "chunks": None,
                    })

            self.self_heal()

    @property
    def missing_chunks(self):
        l = []
        for file in self.files.values():
            l.extend(file.missing_chunks)
        return l

    @property
    def known_chunks(self):
        l = []
        for file in self.files.values():
            l.extend(file.known_chunks)
        return l

    def __dict__(self, local=False, state=False):
        data = {
            "id": self.id,
            "files": dict([(file.filename, file.__dict__()) for file in self.files.values()])
        }
        if local:
            data.update({
                "root": self.root
            })
        return data

    def file_update(self, filedata):
        file = File.from_struct(self, filedata)

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
                    file.log("created")

        else:
            # old version of file we've seen before
            pass

    def chunk_found(self, chunk_id, data):
        self.log("Trying to insert chunk %s into files" % chunk_id)
        for chunk in self.missing_chunks:
            if chunk.id == chunk_id:
                chunk.save_data(data)

    def self_heal(self, known_chunks=None, missing_chunks=None):
        # this could be much more efficient -
        # sort the lists, then go through each list once linearly
        if known_chunks is None:
            known_chunks = self.known_chunks
        if missing_chunks is None:
            missing_chunks = self.missing_chunks

        if known_chunks and missing_chunks:
            self.log("Attempting self-healing")
            for known_chunk in known_chunks:
                for missing_chunk in missing_chunks:
                    if known_chunk.id == missing_chunk.id:
                        self.log("Copying chunk from %s to %s" % (known_chunk.file.filename, missing_chunk.file.filename))
                        missing_chunk.save_data(known_chunk.get_data())

    def log(self, msg):
        log("[%s] %s" % (self.id[:10], msg))

