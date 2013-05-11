from .chunk import Chunk


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
