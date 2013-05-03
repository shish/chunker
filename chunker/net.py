from datetime import datetime
from chunker.util import log


class Peer(object):
    def __init__(self, (host, port)):
        self.host = host
        self.port = int(port)

    def __repr__(self):
        return "Peer((%r, %r))" % (self.host, self.port)

    def get_files(self):
        self.log("Asking for any new files")
        return []

    def log(self, msg):
        log("[%s:%d] %s" % (self.host, self.port, msg))


class Seed(object):
    http_blah = "s"

    def __init__(self):
        self.main = None

    def start(self):
        print "Seeding:"
        pass

    def stop(self):
        pass


class NetManager(object):
    def __init__(self):
        self.peers = []
        self.seed = Seed()
        for addr in self.peers:
            host, _, port = addr.partition(":")
            self.peers.append(Peer((host, port)))

    def start(self):
        self.seed.start()
        print "Looking for missing chunks"
        print self.share.repo.missing_chunks

        print "Looking for new files"
        for peer in self.peers:
            print peer.get_files()

    def stop(self):
        self.seed.stop()

