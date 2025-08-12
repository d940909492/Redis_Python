import socket
import sys
import threading
from datastore import handle_command

def parse_resp_array(data):
    """Parse one RESP array command from data; return (parts, rest_data)."""
    if not data or not data.startswith(b'*'):
        return None, data
    try:
        newline_pos = data.index(b'\r\n')
        num_elements = int(data[1:newline_pos])
        pos = newline_pos + 2
        parts = []
        for _ in range(num_elements):
            if pos >= len(data) or data[pos:pos+1] != b'$':
                return None, data
            newline_pos = data.index(b'\r\n', pos)
            bulk_len = int(data[pos+1:newline_pos])
            start = newline_pos + 2
            end = start + bulk_len
            if end + 2 > len(data):
                return None, data
            parts.append(data[start:end].decode())
            pos = end + 2
        return parts, data[pos:]
    except Exception:
        return None, data

def replica_handler(master_host, master_port, datastore, server_state):
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

    buffer = master_sock.recv(4096)
    if b"FULLRESYNC" in buffer:
        try:
            rdb_start = buffer.index(b'$')
            newline_pos = buffer.index(b'\r\n', rdb_start)
            rdb_len = int(buffer[rdb_start+1:newline_pos])
            total_len = newline_pos + 2 + rdb_len + 2
            buffer = buffer[total_len:]
        except Exception:
            buffer = b""

    while True:
        if not buffer:
            chunk = master_sock.recv(4096)
            if not chunk:
                break
            buffer = chunk

        parts, buffer = parse_resp_array(buffer)
        if parts:
            handle_command(parts, datastore, server_state)
        else:
            more = master_sock.recv(4096)
            if not more:
                break
            buffer += more

    master_sock.close()

def client_handler(client_sock, addr, datastore, server_state):
    print(f"Connect from {addr}")
    buffer = b""
    while True:
        try:
            data = client_sock.recv(4096)
            if not data:
                break
            buffer += data
            while True:
                parts, buffer = parse_resp_array(buffer)
                if not parts:
                    break
                reply = handle_command(parts, datastore, server_state)
                if reply is not None:
                    client_sock.sendall(reply)
        except ConnectionResetError:
            break
    print(f"Closing connection from {addr}")
    client_sock.close()

def main():
    host = "localhost"
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 6380

    datastore = {}
    server_state = {"port": port}

    if "--replicaof" in sys.argv:
        idx = sys.argv.index("--replicaof")
        master_host, master_port = sys.argv[idx+1].split()
        if len(sys.argv) > idx+2:
            master_port = sys.argv[idx+2]
        else:
            master_port = '6379'
        replica_thread = threading.Thread(target=replica_handler, args=(master_host, master_port, datastore, server_state))
        replica_thread.daemon = True
        replica_thread.start()

    server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_sock.bind((host, port))
    server_sock.listen(5)
    print(f"Server listen on {host} {port}")

    while True:
        client_sock, addr = server_sock.accept()
        threading.Thread(target=client_handler, args=(client_sock, addr, datastore, server_state), daemon=True).start()

if __name__ == "__main__":
    main()
