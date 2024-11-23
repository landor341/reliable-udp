"""
Where solution code to hw4 should be written.  No other files should
be modified.
"""

import socket
import io
import time
import typing
import struct
import homework4
import homework4.logging


def send(sock: socket.socket, data: bytes):
    """
    Implementation of the sending logic for sending data over a slow,
    lossy, constrained network.

    Args:
        sock -- A socket object, constructed and initialized to communicate
                over a simulated lossy network.
        data -- A bytes object, containing the data to send over the network.
    """
    sender = RUDP_Service(sock, sendData=data)
    sender.send_all_data()


def recv(sock: socket.socket, dest: io.BufferedIOBase) -> int:
    """
    Implementation of the receiving logic for receiving data over a slow,
    lossy, constrained network.

    Args:
        sock -- A socket object, constructed and initialized to communicate
                over a simulated lossy network.

    Return:
        The number of bytes written to the destination.
    """
    receiver = RUDP_Service(sock, dataDest=dest)
    receiver.receive_data()
    return receiver.last_seq


class RUDP_Packet:
    def __init__(self, header_bytes):
        if len(header_bytes) < 16:
            raise ValueError()
        self.seqNum = int.from_bytes(header_bytes[0:4], "big")
        self.ackNum = int.from_bytes(header_bytes[4:8], "big")

        self.flags = header_bytes[8]
        self.syn = self.flags & 0b01
        self.ack = self.flags & 0b010
        self.fin = self.flags & 0b0100

        self.window = int.from_bytes(header_bytes[9:10], "big")

        if len(header_bytes) == 16:
            self.data = None
        else:
            self.data = header_bytes[16:]


class RUDP_Service:
    def __init__(self, sock, sendData=None, dataDest=None):
        self.sock = sock
        self.fin = False
        self.dup_ack_count = 0
        self.most_recent_ack = 0
        self.send_time_map = {}
        self.last_seq = 0

        self.rtt_buffer = [0.1] * 16
        self.num_rtt_set = 0
        self.rtt_buffer_head = 15
        self.transmission_delay = 0
        self.timeout_multiplier = 1/20

        self.packet_timeout = 1  # Initial RTO for syn stage
        self.cwnd = 1
        self.ssthresh = 32

        self.data_size = homework4.MAX_PACKET - 16

        self.data = sendData
        self.dest = dataDest

    def recv_data(self):
        self.sock.settimeout(self.packet_timeout * 3.5)
        try:
            res = self.sock.recv(homework4.MAX_PACKET)
            return RUDP_Packet(res)
        except socket.timeout:
            # TODO: Deal with timeout congestion
            return None

    def process_next_ack(self):
        # receive next 16 bit RUDP header and process it
        self.sock.settimeout(0)
        try:
            packet = RUDP_Packet(self.sock.recv(16))
            self.processPacket(packet)
            return True
        except BlockingIOError:
            return False

    def send_next_data_packet(self):
        # TODO: Register start time, Update last_seq, Send next data packet with accurate header
        start_of_seq = self.last_seq
        self.last_seq += self.data_size
        self.send_time_map[self.last_seq] = time.time()

        self.sock.send(
            create_rudp_packet(
                self.data[start_of_seq:self.last_seq],
                self.last_seq,
                0,  # ack doesn't matter to receiver
                fin=len(self.data) <= self.last_seq
            )
        )

    def send_ack_packet(self):
        # Send next packet, no data, with correct header
        self.send_time_map[0] = time.time()
        self.sock.send(
            create_rudp_packet(b"", 0, self.last_seq)
        )

    # Method for running the sender instance
    def send_all_data(self):
        while True:
            try:
                self.sock.send(create_rudp_packet(b"", 0, 0, syn=1))
                self.send_time_map[0] = time.time()
                self.sock.settimeout(self.packet_timeout)
                data = self.sock.recv(16)  # Recv syn,ack
                self.packet_timeout = 0
                self.processPacket(RUDP_Packet(data))
                break
            except socket.timeout:
                self.packet_timeout *= 1.2
        
        # Send ack packet
        while self.most_recent_ack < len(self.data):
            if time.time() - self.send_time_map[self.last_seq] > self.transmission_delay:
                if (self.last_seq - self.most_recent_ack) < self.cwnd * self.data_size:
                    if self.last_seq < len(self.data):
                        self.send_next_data_packet()
            while self.process_next_ack():
                pass  # Loop through receiving data until there is none left, processing each packet
            if time.time() - self.send_time_map[self.last_seq] > self.packet_timeout:  # Last packet timed out
                self.last_seq = self.most_recent_ack
                self.dup_ack_count = 0
                self.ssthresh //= 2
                self.cwnd = 1
                self.transmission_delay = self.packet_timeout / 2.3

        return


    # Method running the receiver instance
    def receive_data(self):
        self.sock.settimeout(60)  # We'll sit here to wait for connection to start pretty much as long as needed
        self.sock.recv(16)
        self.send_ack_packet()

        while True:
            if self.fin:
                while True:
                    self.sock.settimeout(self.packet_timeout * 2.5)
                    try:
                        self.sock.recv(homework4.MAX_PACKET)
                        self.send_ack_packet() # keep looping if the sender is still trying to send
                    except Exception:
                        return
            data = self.recv_data()
            if data is not None:
                self.processPacket(data)
            self.send_ack_packet()

    def processPacket(self, packet: RUDP_Packet):
        # Calculate packet timeout using estimated RTT
        self.rtt_buffer_head = (self.rtt_buffer_head + 1) % 16
        if self.num_rtt_set < 16:
            self.num_rtt_set += 1
            self.packet_timeout *= self.num_rtt_set / 16
        else:
            self.packet_timeout -= self.rtt_buffer[self.rtt_buffer_head]
        self.rtt_buffer[self.rtt_buffer_head] = (time.time() - self.send_time_map[packet.ackNum]) * self.timeout_multiplier / 16
        self.packet_timeout += self.rtt_buffer[self.rtt_buffer_head]
        self.packet_timeout *= 16 / self.num_rtt_set

        # Update cwnd
        if self.cwnd < self.ssthresh:
            self.cwnd *= 2
        else:
            self.cwnd += 1

        # Process given data
        if self.dest:
            if packet.seqNum > self.last_seq + self.data_size:
                pass  # Lost a packet and will send duplicate ack.
            elif packet.seqNum > self.last_seq and packet.data:
                # Save packet data as receiver
                if packet.fin:
                    self.fin = True
                self.dest.write(packet.data)
                self.dest.flush()
                self.last_seq = packet.seqNum
        elif self.data and self.most_recent_ack == packet.ackNum: # Is sender and got dup ack
            # Manage dup acks
            self.dup_ack_count += 1
            if self.dup_ack_count == 3: # CONGESTION CONTROL: 3 dup acks
                self.last_seq = self.most_recent_ack
                self.ssthresh = self.cwnd // 2
                self.cwnd = self.ssthresh
                self.dup_ack_count = 0
        else: # Is sender and received new ack recently
            self.dup_ack_count = 0
            self.most_recent_ack = packet.ackNum


def create_rudp_packet(data, seq_num, ack_num, window=0, syn=0, ack=0, fin=0):
    '''
        data is a required set of bytes
        seq_num and ack_num are required 32 bit numbers
        window is an optional 32 bit number
        syn, ack, and fin are optional 1 bit flags
    '''
    return (
            seq_num.to_bytes(4, byteorder='big')
            + ack_num.to_bytes(4, byteorder='big')
            + (
                (fin << 2) + (ack << 1) + syn
            ).to_bytes(1, byteorder='big')
            + (0).to_bytes(7, byteorder='big')  # 7 filler bytes to make header 16 bytes
            + data
    )
