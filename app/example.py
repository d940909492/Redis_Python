import dataclasses
import socket
import threading
import datetime
import argparse
from typing import Any, Dict, Optional, cast, List
EMPTY_RDB = bytes.fromhex(
    "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2"
)
@dataclasses.dataclass
class Args:
    port: int
    replicaof: Optional[str]
def parse_next(data: bytes):
    first, data = data.split(b"\r\n", 1)
    match first[:1]:
        case b"*":
            value = []
            l = int(first[1:].decode())
            for _ in range(l):
                item, data = parse_next(data)
                value.append(item)
            return value, data
        case b"$":
            l = int(first[1:].decode())
            blk = data[:l]
            data = data[l + 2 :]
            return blk, data
        case b"+":
            return first[1:].decode(), data
        case _:
            raise RuntimeError(f"Parse not implemented: {first[:1]}")
def encode_resp(data: Any, trailing_crlf: bool = True) -> bytes:
    if isinstance(data, bytes):
        return b"$%b\r\n%b%b" % (
            str(len(data)).encode(),
            data,
            b"\r\n" if trailing_crlf else b"",
        )
    if isinstance(data, str):
        return b"+%b\r\n" % (data.encode(),)
    if data is None:
        return b"$-1\r\n"
    if isinstance(data, list):
        return b"*%b\r\n%b" % (
            str(len(data)).encode(),
            b"".join(map(encode_resp, data)),
        )
    raise RuntimeError(f"Encode not implemented: {data}")
@dataclasses.dataclass
class Value:
    value: Any
    expiry: Optional[datetime.datetime]
db: Dict[Any, Value] = {}
@dataclasses.dataclass
class Replication:
    master_replid: str
    master_repl_offset: int
    connected_replicas: List[socket.socket]
replication = Replication(
    master_replid="8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb",
    master_repl_offset=0,
    connected_replicas=[],
)
def handle_conn(args: Args, conn: socket.socket, is_replica_conn: bool = False):
    data = b""
    while data or (data := conn.recv(4096)):
        value, data = parse_next(data)
        match value:
            case [b"PING"]:
                conn.send(encode_resp("PONG"))
            case [b"ECHO", s]:
                conn.send(encode_resp(s))
            case [b"SET", k, v]:
                for rep in replication.connected_replicas:
                    rep.send(encode_resp(value))
                db[k] = Value(
                    value=v,
                    expiry=None,
                )
                if not is_replica_conn:
                    conn.send(encode_resp("OK"))
            case [b"SET", k, v, b"px", expiry_ms]:
                for rep in replication.connected_replicas:
                    rep.send(encode_resp(value))
                now = datetime.datetime.now()
                expiry_ms = datetime.timedelta(
                    milliseconds=int(expiry_ms.decode()),
                )
                db[k] = Value(
                    value=v,
                    expiry=now + expiry_ms,
                )
                if not is_replica_conn:
                    conn.send(encode_resp("OK"))
            case [b"GET", k]:
                now = datetime.datetime.now()
                value = db.get(k)
                if value is None:
                    conn.send(encode_resp(None))
                elif value.expiry is not None and now >= value.expiry:
                    db.pop(k)
                    conn.send(encode_resp(None))
                else:
                    conn.send(encode_resp(value.value))
            case [b"INFO", b"replication"]:
                if args.replicaof is None:
                    info = f"""\
role:master
master_replid:{replication.master_replid}
master_repl_offset:{replication.master_repl_offset}
""".encode()
                else:
                    info = b"""role:slave\n"""
                conn.send(encode_resp(info))
            case [b"REPLCONF", b"listening-port", port]:
                conn.send(encode_resp("OK"))
            case [b"REPLCONF", b"capa", b"psync2"]:
                conn.send(encode_resp("OK"))
            case [b"PSYNC", replid, offset]:
                conn.send(
                    encode_resp(
                        f"FULLRESYNC "
                        f"{replication.master_replid} "
                        f"{replication.master_repl_offset}"
                    )
                )
                conn.send(encode_resp(EMPTY_RDB, trailing_crlf=False))
                replication.connected_replicas.append(conn)
            case _:
                raise RuntimeError(f"Command not implemented: {value}")
def main(args: Args):
    server_socket = socket.create_server(
        ("localhost", args.port),
        reuse_port=True,
    )
    if args.replicaof is not None:
        (host, port) = args.replicaof.split(" ")
        port = int(port)
        master_conn = socket.create_connection((host, port))
        # Handshake PING
        master_conn.send(encode_resp([b"PING"]))
        resp, _ = parse_next(master_conn.recv(4096))
        assert resp == "PONG"
        # Handshake REPLCONF listening-port
        master_conn.send(
            encode_resp(
                [
                    b"REPLCONF",
                    b"listening-port",
                    str(args.port).encode(),
                ]
            )
        )
        resp, _ = parse_next(master_conn.recv(4096))
        assert resp == "OK"
        # Handshake REPLCONF capabilities
        master_conn.send(encode_resp([b"REPLCONF", b"capa", b"psync2"]))
        resp, _ = parse_next(master_conn.recv(4096))
        assert resp == "OK"
        # Handshake PSYNC
        master_conn.send(encode_resp([b"PSYNC", b"?", b"-1"]))
        resp, _ = parse_next(master_conn.recv(4096))
        assert isinstance(resp, str)
        assert resp.startswith("FULLRESYNC")
        # Receive db
        resp_bs = master_conn.recv(4096)
        print(f"Handshake with master completed: {resp=}")
        threading.Thread(
            target=handle_conn,
            args=(args, master_conn, True),
            daemon=True,
        ).start()
    with server_socket:
        while True:
            (conn, _) = server_socket.accept()
            threading.Thread(
                target=handle_conn,
                args=(
                    args,
                    conn,
                    False,
                ),
                daemon=True,
            ).start()
if __name__ == "__main__":
    args = argparse.ArgumentParser()
    args.add_argument("--port", type=int, default=6379)
    args.add_argument("--replicaof", required=False)
    main(cast(Args, args.parse_args()))