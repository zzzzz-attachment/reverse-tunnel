"""Bidirectional byte pumping between a WSConn and a raw socket."""

import socket
import threading


def _close_sock(sock):
    try:
        sock.shutdown(socket.SHUT_RDWR)
    except Exception:
        pass
    try:
        sock.close()
    except Exception:
        pass


def pump_ws_to_sock(ws, sock, stop):
    try:
        while not stop.is_set():
            kind, data = ws.recv()
            if kind == "close":
                break
            if kind == "binary":
                sock.sendall(data)
            elif kind == "text":
                sock.sendall(data.encode())
    except Exception:
        pass
    finally:
        stop.set()
        _close_sock(sock)


def pump_sock_to_ws(sock, ws, stop):
    try:
        while not stop.is_set():
            data = sock.recv(65536)
            if not data:
                break
            ws.send_bytes(data)
    except Exception:
        pass
    finally:
        stop.set()
        ws.close()


def bridge(ws, sock):
    """Pump both directions until either side closes."""
    stop = threading.Event()
    t1 = threading.Thread(target=pump_ws_to_sock, args=(ws, sock, stop), daemon=True)
    t2 = threading.Thread(target=pump_sock_to_ws, args=(sock, ws, stop), daemon=True)
    t1.start()
    t2.start()
    t1.join()
    t2.join()
