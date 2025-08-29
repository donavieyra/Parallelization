import pickle, gzip, struct

HDR = struct.Struct("!Q")

def send_msg(sock, obj):
    data = gzip.compress(pickle.dumps(obj))
    sock.sendall(HDR.pack(len(data)) + data)

def recv_msg(sock):
    hdr = sock.recv(HDR.size)
    if not hdr:
        return None
    (length,) = HDR.unpack(hdr)
    data = b""
    while len(data) < length:
        packet = sock.recv(length - len(data))
        if not packet:
            return None
        data += packet
    return pickle.loads(gzip.decompress(data))
