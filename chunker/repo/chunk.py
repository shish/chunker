import os
import hashlib
#if sys.version_info < (3, 4):
#   import sha3

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
            data = fp.read(self.length)
            data = self.file.repo.encrypt(data)
            return data
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
        data = self.file.repo.decrypt(data)
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
