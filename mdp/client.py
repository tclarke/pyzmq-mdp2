import zmq
from zmq.eventloop.zmqstream import ZMQStream
from zmq.eventloop.ioloop import PeriodicCallback,DelayedCallback
import time
import logging

def msg_str(msg):
    def f(s):
        if len(s) > 10: return "%s...[%i]" % (s[:10],len(s))
        return s
    return map(f, msg)

MDPC_VER='MDPC02'

class MDPClient(ZMQStream):
    def __init__(self, broker, io_loop=None):
        self.ctx = zmq.Context()
        sock = self.ctx.socket(zmq.REQ)
        ZMQStream.__init__(self, sock, io_loop)
        self.on_recv(self.on_message)
        self.connect(broker)

    def send_request(self, service, msg):
        #rqst = [MDPC_VER, b'\x01', service]
        rqst = [MDPC_VER, service]
        if isinstance(msg, list):
            rqst.extend(msg)
        else:
            rqst.append(msg)
        logging.debug("Sending request: %s" % msg_str(rqst))
        self.send_multipart(rqst)

    def on_reply(self, msg, service, more_available):
        raise NotImplementedError("Derived classes must implement on_reply")

    def on_message(self, msg, *args):
        try:
### The MDP/0.2 tag on libmdp (reference implementation) doesn't seem to implement the
### complete client protocol. I believe these checks should correspond to the full protocol
### so I just commecnted them out for now until we figure out what the status of libmdp is.
#            if msg[1] != MDPC_VER:
#                logging.warning("Invalid message version: %s" % msg_str(msg))
#                return
#            if msg[2] == b'\x01':
#                logging.warning("Unexpected response: %s" % msg_str(msg))
#                return
#            elif msg[2] == b'\x02':
#                logging.info("Received PARTIAL reply.")
#                self.on_reply(msg[3:], msg[2], True)
#            elif msg[2] == b'\x03':
#                logging.info("Received FINAL reply.")
#                self.on_reply(msg[3:], msg[2], False)
#            else:
#                logging.warning("ERROR: Invalid or unexpected message: %s" % msg_str(msg))
            if msg[0] != MDPC_VER:
                logging.warning("Invalid message version: %s" % msg_str(msg))
                return
            logging.info("Received FINAL reply.")
            self.on_reply(msg[2:], msg[1], False)
        except IndexError,e:
            logging.warning("ERROR: Invalid message [%s]: %s" % (e,msg_str(msg)))
