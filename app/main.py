import socket
import sys
from datastore import handle_command

def parse_resp_array(data):
    """Parses a single RESP array from data, returns (parts, rest_of_data)."""
    if not data or not data.startswith(b'*'):
        return None, data
    try:
        newline_pos = data.index(b'\r\n')
    except ValueError:
        return None, data
    num_elements = int(data[1:newline_pos])
    pos = newline_pos + 2
    parts = []
    for _ in range(num_elements):
        if pos >= len(data) or data[pos:pos+1] != b'$':
            return None, data
        try:
            newline_pos = data.index(b'\r\n', pos)
        except ValueError:
            return None, data
        bulk_len = int(data[pos+1:newline_pos])
        start = newline_pos + 2
        end = start + bulk_len
        if end + 2 > len(data):
            return None, data 
        parts.append(data[start:end].decode())
        pos = end + 2
    return parts, data[pos:]


def replica_main(master_host, master_port, datastore, server_state):
    """Connects to master and handles replication stream."""
    master_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    master_sock.connect((master_host, int(master_port)))
    print(f"Connected to master at {master_host}:{master_port}")

    master_sock.sendall(b"*1\r\n$4\r\nPING\r\n")
    master_sock.recv(1024)
    master_sock.sendall(f"*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n${len(str(server_state['port']))}\r\n{server_state['port']}\r\n".encode())
    master_sock.recv(1024)
    master_sock.sendall(b"*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n")
    master_sock.recv(1024)
    master_sock.sendall(b"*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n")

    first_reply = master_sock.recv(4096)
    if b"FULLRESYNC" in first_reply:
        try:
            rdb_start = first_reply.index(b'$')
            newline_pos = first_reply.index(b'\r\n', rdb_start)
            rdb_len = int(first_reply[rdb_start+1:newline_pos])
            total_len = newline_pos + 2 + rdb_len + 2
            first_reply = first_reply[total_len:]
        except Exception:
            first_reply = b""

    buffer = first_reply
    while True:
        chunk = master_sock.recv(4096)
        if not chunk:
            break
        buffer += chunk
        while True:
            parts, buffer = parse_resp_array(buffer)
            if not parts:
                break
            handle_command(parts, datastore, server_state)

    master_sock.close()


if __name__ == "__main__":
    datastore = {}
    server_state = {"port": 6380}
    replica_main(sys.argv[1], sys.argv[2], datastore, server_state)
