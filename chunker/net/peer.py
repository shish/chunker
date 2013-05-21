from socket import *

class Peer(object):
    def __init__(self, addr):
        self.addr = addr
        self.socket = socket(AF_INET, SOCK_DGRAM)
        self.socket.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
        self.socket.setsockopt(SOL_SOCKET, SO_BROADCAST, 1)

    def __repr__(self):
        return "Peer(%r)" % (self.addr, )

    def __cmp__(self, other):
        return cmp(self.addr, other.addr)

    def to_struct(self, state=False):
        return {
            "type": "direct-udp",
            "host": self.addr[0],
            "port": self.addr[1],
        }

    def send(self, msg):
        self.socket.sendto(msg, self.addr)

    def recv(self):
        data, ancdata, flags, addr = self.socket.recvmsg()
        return data

    def fileno(self):
        return self.socket.fileno()
