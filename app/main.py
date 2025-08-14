import socket
import threading
import argparse
import time
from .datastore import RedisDataStore
from .command_handler import handle_command, WRITE_COMMANDS
from . import protocol

EMPTY_RDB_HEX = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc226404000fa0c616f662d707265616d626c65c000fffe00f7e03ac95225"
EMPTY_RDB_CONTENT = bytes.fromhex(EMPTY_RDB_HEX)

def parse_commands_from_buffer(buffer):
    """
    Parses multiple commands from a stream buffer.
    Returns a list of raw command byte strings and the remaining, unprocessed buffer.
    """
    commands = []
    current_pos = 0
    while current_pos < len(buffer):
        start_pos = current_pos
        if not buffer[start_pos:].startswith(b'*'):
            break
        
        crlf1 = buffer.find(b'\r\n', start_pos)
        if crlf1 == -1: break

        try:
            num_elements = int(buffer[start_pos+1:crlf1])
            current_pos = crlf1 + 2

            for _ in range(num_elements):
                if not buffer[current_pos:].startswith(b'$'): raise ValueError("Expected bulk string prefix")
                crlf2 = buffer.find(b'\r\n', current_pos)
                if crlf2 == -1: raise ValueError("Incomplete bulk string length")
                length = int(buffer[current_pos+1:crlf2])
                
                data_start = crlf2 + 2
                data_end = data_start + length
                
                if len(buffer) < data_end + 2: raise ValueError("Incomplete bulk string data")
                
                current_pos = data_end + 2

            commands.append(buffer[start_pos:current_pos])
        except (ValueError, IndexError):
            break
            
    return commands, buffer[current_pos:]

def handle_client(client_socket, client_address, datastore, server_state):
    print(f"Connect from {client_address}")
    in_transaction = False
    transaction_queue = []
    is_replica = False

    while True:
        try:
            request_bytes = client_socket.recv(1024)
            if not request_bytes: break

            parts = request_bytes.strip().split(b'\r\n')
            command_name = parts[2].decode().upper()

            if in_transaction and command_name not in ["EXEC", "DISCARD", "MULTI"]:
                transaction_queue.append((parts, request_bytes))
                client_socket.sendall(protocol.format_simple_string("QUEUED"))
                continue

            if command_name == "MULTI":
                in_transaction = True
                transaction_queue = []
                client_socket.sendall(protocol.format_simple_string("OK"))
            elif command_name == "EXEC":
                if not in_transaction:
                    client_socket.sendall(protocol.format_error("EXEC without MULTI"))
                else:
                    responses = []
                    bytes_to_propagate = []
                    for queued_parts, original_bytes in transaction_queue:
                        responses.append(handle_command(queued_parts, datastore, server_state))
                        if queued_parts[2].decode().upper() in WRITE_COMMANDS:
                            bytes_to_propagate.append(original_bytes)
                    
                    if server_state["role"] == "master":
                        for original_bytes in bytes_to_propagate:
                            server_state["master_repl_offset"] += len(original_bytes)
                            for replica_socket in server_state["replicas"]:
                                replica_socket.sendall(original_bytes)

                    client_socket.sendall(protocol.format_array(responses))
                    in_transaction = False
                    transaction_queue = []
            elif command_name == "DISCARD":
                if not in_transaction:
                    client_socket.sendall(protocol.format_error("DISCARD without MULTI"))
                else:
                    in_transaction = False
                    transaction_queue = []
                    client_socket.sendall(protocol.format_simple_string("OK"))
            elif command_name == "REPLCONF":
                if len(parts) > 5 and parts[4].decode().upper() == "ACK":
                    ack_offset = int(parts[6].decode())
                    server_state["replica_acks"][client_socket] = ack_offset
                else:
                    client_socket.sendall(handle_command(parts, datastore, server_state))
            elif command_name == "WAIT":
                num_replicas_to_wait_for = int(parts[4].decode())
                timeout_ms = int(parts[6].decode())
                
                if server_state["master_repl_offset"] == 0:
                    client_socket.sendall(protocol.format_integer(len(server_state["replicas"])))
                    continue

                getack_command = protocol.format_array([
                    protocol.format_bulk_string(b"REPLCONF"),
                    protocol.format_bulk_string(b"GETACK"),
                    protocol.format_bulk_string(b"*")
                ])
                for replica_socket in server_state["replicas"]:
                    replica_socket.sendall(getack_command)
                
                start_time = time.time()
                acked_replicas = 0
                while True:
                    with datastore.lock:
                        current_acked = sum(1 for offset in server_state["replica_acks"].values() if offset >= server_state["master_repl_offset"])
                    acked_replicas = current_acked
                    
                    if acked_replicas >= num_replicas_to_wait_for:
                        break
                    
                    if (time.time() - start_time) * 1000 > timeout_ms:
                        break
                    
                    time.sleep(0.01)
                
                client_socket.sendall(protocol.format_integer(acked_replicas))
            else:
                response = handle_command(parts, datastore, server_state)
                if isinstance(response, tuple):
                    response_bytes, action = response
                    client_socket.sendall(response_bytes)
                    if action == "SEND_RDB_FILE":
                        rdb_response = f"${len(EMPTY_RDB_CONTENT)}\r\n".encode() + EMPTY_RDB_CONTENT
                        client_socket.sendall(rdb_response)
                        if server_state["role"] == "master":
                            is_replica = True
                            server_state["replicas"].append(client_socket)
                            server_state["replica_acks"][client_socket] = 0
                else:
                    client_socket.sendall(response)
                    if command_name in WRITE_COMMANDS and server_state["role"] == "master":
                        server_state["master_repl_offset"] += len(request_bytes)
                        for replica_socket in server_state["replicas"]:
                            replica_socket.sendall(request_bytes)

        except (IndexError, ConnectionResetError, ValueError):
            break
    
    print(f"Closing connection from {client_address}")
    if is_replica:
        server_state["replicas"].remove(client_socket)
        if client_socket in server_state["replica_acks"]:
            del server_state["replica_acks"][client_socket]
    client_socket.close()

def connect_to_master(server_state, replica_port, datastore):
    master_host, master_port = server_state["master_host"], server_state["master_port"]
    try:
        master_socket = socket.create_connection((master_host, master_port))
        print(f"Connected to master at {master_host}:{master_port}")
        master_socket.sendall(protocol.format_array([protocol.format_bulk_string(b"PING")]))
        master_socket.recv(1024)
        master_socket.sendall(protocol.format_array([protocol.format_bulk_string(b"REPLCONF"), protocol.format_bulk_string(b"listening-port"), protocol.format_bulk_string(str(replica_port).encode())]))
        master_socket.recv(1024)
        master_socket.sendall(protocol.format_array([protocol.format_bulk_string(b"REPLCONF"), protocol.format_bulk_string(b"capa"), protocol.format_bulk_string(b"psync2")]))
        master_socket.recv(1024)
        master_socket.sendall(protocol.format_array([protocol.format_bulk_string(b"PSYNC"), protocol.format_bulk_string(b"?"), protocol.format_bulk_string(b"-1")]))
        
        buffer = b""
        while True:
            data = master_socket.recv(1024)
            if not data: break
            buffer += data
            if b'$' in buffer and b'\r\n' in buffer: break
        
        rdb_start = buffer.find(b'$') + 1
        rdb_end_of_length = buffer.find(b'\r\n', rdb_start)
        rdb_length = int(buffer[rdb_start:rdb_end_of_length])
        rdb_data_start = rdb_end_of_length + 2
        
        while len(buffer) < rdb_data_start + rdb_length:
            buffer += master_socket.recv(1024)
        
        buffer = buffer[rdb_data_start + rdb_length:]
        
        bytes_processed = 0
        while True:
            commands, buffer = parse_commands_from_buffer(buffer)
            for command_bytes in commands:
                parts = command_bytes.strip().split(b'\r\n')
                command_name = parts[2].decode().upper()
                
                if command_name == "REPLCONF" and len(parts) > 5 and parts[4].decode().upper() == "GETACK":
                    ack_response = protocol.format_array([
                        protocol.format_bulk_string(b"REPLCONF"),
                        protocol.format_bulk_string(b"ACK"),
                        protocol.format_bulk_string(str(bytes_processed).encode())
                    ])
                    master_socket.sendall(ack_response)
                else:
                    handle_command(parts, datastore, server_state)
                
                bytes_processed += len(command_bytes)

            more_data = master_socket.recv(1024)
            if not more_data: break
            buffer += more_data
            
    except Exception as e:
        print(f"Error in master connection: {e}")

def main():
    print("Redis server start...")
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=6379)
    parser.add_argument("--replicaof", type=str, help="Start server as a replica of a master.")
    args = parser.parse_args()
    
    datastore = RedisDataStore()
    
    server_state = {
        "master_replid": "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb",
        "master_repl_offset": 0,
        "replicas": [],
        "replica_acks": {},
    }
    
    if args.replicaof:
        server_state["role"] = "slave"
        master_host, master_port = args.replicaof.split()
        server_state["master_host"] = master_host
        server_state["master_port"] = int(master_port)
        handshake_thread = threading.Thread(target=connect_to_master, args=(server_state, args.port, datastore))
        handshake_thread.start()
    else:
        server_state["role"] = "master"
    
    server_socket = socket.create_server(("localhost", args.port))
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    print(f"Server listen on localhost {args.port}")
    
    while True:
        client_socket, client_address = server_socket.accept()
        client_thread = threading.Thread(
            target=handle_client,
            args=(client_socket, client_address, datastore, server_state),
            daemon=True
        )
        client_thread.start()

if __name__ == "__main__":
    main()