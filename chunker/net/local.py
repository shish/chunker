from datetime import datetime
from pydht import DHT
from time import sleep
from socket import *
from threading import Thread
import stun
import json
import logging

from chunker.net.peerfinder import PeerFinder
from chunker.net.peer import Peer


log = logging.getLogger(__name__)


class LocalPeerFinder(PeerFinder):
    def __init__(self, core):
        PeerFinder.__init__(self, core)

        self.socket = socket(AF_INET, SOCK_DGRAM)
        self.socket.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
        self.socket.setsockopt(SOL_SOCKET, SO_BROADCAST, 1)
        self.socket.bind(("0.0.0.0", 54545))

        self.sender = Thread(target=self.run_send, name="LocalPeerFinder[Send]")
        self.recver = Thread(target=self.run_recv, name="LocalPeerFinder[Recv]")

        self.sender.daemon = True
        self.recver.daemon = True

    def start(self):
        self.sender.start()
        self.recver.start()

    def run_send(self):
        while True:
            log.info("Broadcasting local repos")
            for repo in self.core.repos.values():
                self.socket.sendto(
                    repo.encrypt(repo.uuid.decode("hex")),
                    ("255.255.255.255", 54545)
                )
            sleep(5)  # low for debugging, probably want this higher

    def run_recv(self):
        while True:
            raw_data, addr = self.socket.recvfrom(4096)
            log.info("Got possible repo broadcast from %s" % (addr, ))
            try:
                for repo in self.core.repos.values():
                    data = repo.decrypt(raw_data).encode("hex").lower()
                    if data == repo.uuid:
                        repo.add_peer(Peer(addr))
            except Exception as e:
                print e


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    class Core(object):
        def __init__(self):
            self.repos = []

    class Repo(object):
        def __init__(self, uuid):
            self.uuid = uuid

        def encrypt(self, data):
            return data

        def decrypt(self, data):
            return data

        def add_peer(self, peer):
            logging.info("Got new peer for %s: %r" % (self.uuid, peer))

    c = Core()
    c.repos.append(Repo("b17081907ad57afb25c62e9970702381ea9c721745f42f6239ff0d5fbd26d309"))
    l = LocalPeerFinder(c)
    l.start()
    print "Hit enter to exit"
    raw_input()
