import os

from .fileversion import FileVersion

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

    def get_chunks(self):
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
        fp = file(self.fullpath)
        while not eof:
            data = fp.read(bite_size)
            if not data:
                break
            data = encrypt(data, self.repo.key)
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
