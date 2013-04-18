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

MDPW_VER='MDPW02'

class MDPWorker(ZMQStream):
    def __init__(self, broker, service, io_loop=None):
        """Create and setup an MDP worker.
           @param broker A string containing the broker's URL
           @param service A string containing the service name
           @param io_loop An existing I/O loop object. If None, the default will be used.
        """
        self.service=service
        self._broker = broker

        self.ctx = zmq.Context()
        sock = self.ctx.socket(zmq.DEALER)
        ZMQStream.__init__(self, sock, io_loop)
        # last watchdog timer tick
        self.watchdog = 0
        # connection callback one-shot
        self._conncb = DelayedCallback(self.send_ready, 3000, self.io_loop)
        # heartbeat callback..runs continuous when connected
        self._hbcb = PeriodicCallback(self.send_heartbeat, 2000, self.io_loop)
        # number of connection attempts
        self._conn_attempt = 0
        # waiting to connect state
        self._waiting_to_connect = True
        # have we been disconnected? (flags a reconnect attempt)
        self.disconnected = False

        # connect the socket and send a READY when the io_loop starts
        self.connect(self._broker)
        self._conncb.start()

    def reset_watchdog(self):
        """Private method used to reset the HEARTBEAT watchdog
        """
        self.watchdog = time.time()

    def disconnect(self):
        """Disconnect from the broker.
        """
        logging.info("Disconnected from broker")
        self.on_recv(None) # stop message processing
        self._conncb.stop() # stop any pending reconnect
        self._hbcb.stop() # stop heartbeats
        self.disconnected = True
        self.io_loop.stop() # stop the I/O loop. If it's used by something else, the caller can restart it

    def reconnect(self):
        """Try to reconnect to the broker.
        """
        if self.disconnected: # don't try and reconnect, we got an explicit disconnect
            return
        logging.info("Attempting to reconnect to broker")
        self._hbcb.stop()
        self._conn_attempt = 0
        self._waiting_to_connect = True
        try:
            self.connect(self._broker)
        except ZMQError:
            logging.exception()
            self.io_loop.stop()
            return
        self._conncb.start()

    def send_ready(self):
        """Send a READY message.
        """
        if not self._waiting_to_connect:
            # connected already
            return
        if self.disconnected: # don't try and connect, we got an explicit disconnect
            return
        logging.debug("Sending READY")
        if self._conn_attempt >= 10:
            logging.error("10 connection attempts have failed. Giving up.")
            return
        self._conn_attempt += 1
        logging.debug("Connection attempt %i" % self._conn_attempt)
        rdy = [b'', MDPW_VER, b'\x01', self.service]
        self.on_recv(self.on_message)
        self.send_multipart(rdy)
        # There is no reply to READY so
        # we must assume we are connected unless we see a DISCONNECT
        self._waiting_to_connect = False
        self._disconed = False
        self.reset_watchdog()
        self._hbcb.start()

    def send_reply(self, client, msg, partial=False):
        """Send a reply to a client. This is typically called from on_request()
           @param client The client identifier as passed to on_request()
           @param msg The message to send to the client. If this is a list, it's appended to the multipart;
                      otherwise it is converted to a string and sent as a single frame.
           @param partial If this is True, the message is sent as a PARTIAL and at least one
                          more call must be made to send_reply(). Otherwise a FINAL is sent and
                          not more calls should be made to send_reply() until another request is processed.
                   
        """
        self._hbcb.stop() # stop while sending other messages
        if partial:
            rply = [b'', MDPW_VER, b'\x03', client, b'']
        else:
            rply = [b'', MDPW_VER, b'\x04', client, b'']
        if isinstance(msg, list):
            rply.extend(msg)
        else:
            rply.append(msg)
        try:
            logging.debug("Sending reply: %s" % msg_str(rply))
            self.send_multipart(rply)
        except BaseException,e:
            logging.error("Error sending reply: " % e)
        self._hbcb.start() # restart heartbeats

    def send_heartbeat(self):
        """Send a HEARTBEAT message to the broker.
        """
        logging.debug("Sending heartbeat")
        try:
            self.send_multipart([b'', MDPW_VER, b'\x05'])
        except IOError:
            # stream is closed
            logging.info("Broker is lost")
            self.reconnect()
        if time.time() - self.watchdog >= 10.:
            logging.info("Broker is lost")
            self.reconnect()

    def on_request(self, msg, client):
        """REQUEST processor which must be implementd in a derived class.
           @param msg The message sent in the REQUEST. This will be a list of strings with
                      the MDP frames removed.
           @param client The client ID which is passed to send_reply()
        """
        raise NotImplementedError("Derived classes must implement on_request")

    def on_message(self, msg, *args):
        """Process a message from the broker.
           @param msg The list of frames in the message.
           @param *args Some versions of ZMQStream seem to send other stuff sometimes. We don't
                        care about the values, this is here to prevent an exception in these cases.
        """
        try:
            logging.debug("Received: %s" % msg)
            if msg[1] != MDPW_VER:
                logging.warning("Invalid message version: %s" % msg_str(msg))
                return
            if self._waiting_to_connect:
                # we haven't sent a READY so anything we receive might be leftover in the 0mq buffers
                # we'll just ignore it and carry on
                return
            if msg[2] == b'\x01': # READY
                self.reset_watchdog() # acts as a heartbeat
                # some brokers seem to respond to READY with another READY
                # seems to be an older draft of the spec? We'll just ignore it and carry on
            elif msg[2] == b'\x02': # REQUEST
                self.reset_watchdog() # acts as a heartbeat
                self.on_request(msg[5:], msg[3])
            elif msg[2] == b'\x05': # HEARTBEAT
                self.reset_watchdog()
            elif msg[2] == b'\x06': # DISCONNECT
                self.disconnect()
            else:
                logging.warning("ERROR: Invalid or unexpected message: %s" % msg_str(msg))
        except IndexError,e:
            logging.warning("ERROR: Invalid message [%s]: %s" % (e,msg_str(msg)))
