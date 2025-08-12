import socket
import threading
import argparse
from .datastore import RedisDataStore
from .command_handler import handle_command, WRITE_COMMANDS
from . import protocol

EMPTY_RDB_HEX = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc226404000fa0c616f662d707265616d626c65c000fffe00f7e03ac95225"
EMPTY_RDB_CONTENT = bytes.fromhex(EMPTY_RDB_HEX)

def handle_client(client_socket, client_address, datastore, server_state):
    print(f"Connect from {client_address}")
    in_transaction = False
    transaction_queue = []

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
                    for queued_parts, original_bytes in transaction_queue:
                        responses.append(handle_command(queued_parts, datastore, server_state))
                        if queued_parts[2].decode().upper() in WRITE_COMMANDS and server_state["role"] == "master":
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
            elif command_name == "BLPOP":
                key = parts[4]
                timeout = float(parts[6].decode())
                response = b""
                with datastore.lock:
                    item = datastore.get_item(key)
                    if item and item[0] == 'list' and item[1]:
                        popped = item[1].pop(0)
                        response = protocol.format_array([protocol.format_bulk_string(key), protocol.format_bulk_string(popped)])
                    else:
                        condition = datastore.get_condition_for_key(key)
                        was_notified = condition.wait(timeout=None if timeout == 0 else timeout)
                        if was_notified:
                            item = datastore.get_item(key)
                            if item and item[0] == 'list' and item[1]:
                                popped = item[1].pop(0)
                                response = protocol.format_array([protocol.format_bulk_string(key), protocol.format_bulk_string(popped)])
                            else: response = protocol.format_bulk_string(None)
                        else: response = protocol.format_bulk_string(None)
                client_socket.sendall(response)
            elif command_name == "XREAD" and b'block' in [p.lower() for p in parts]:
                response = handle_command(parts, datastore, server_state)
                if response == protocol.format_bulk_string(None):
                    try:
                        block_idx = parts.index(b'block')
                        timeout_ms = int(parts[block_idx + 2].decode())
                        streams_idx = parts.index(b'streams')
                        num_keys = (len(parts) - streams_idx - 1) // 4
                        keys_start_idx, ids_start_idx = streams_idx + 2, streams_idx + 2 + (num_keys * 2)
                        keys = parts[keys_start_idx:ids_start_idx:2]
                        start_ids_str = [p.decode() for p in parts[ids_start_idx::2]]
                    except (ValueError, IndexError):
                        client_socket.sendall(protocol.format_error("syntax error"))
                        continue

                    def parse_id(id_str):
                        if id_str == '$': return '$'
                        return tuple(map(int, id_str.split('-')))

                    resolved_start_ids = {}
                    with datastore.lock:
                        for i, key in enumerate(keys):
                            id_val = start_ids_str[i]
                            if id_val == '$':
                                item = datastore.get_item(key)
                                resolved_start_ids[key] = parse_id(item[1][-1][0].decode()) if item and item[1] else (0,0)
                            else:
                                resolved_start_ids[key] = parse_id(id_val)
                    
                    condition = datastore.get_condition_for_key(keys[0])
                    with datastore.lock:
                        was_notified = condition.wait(timeout=timeout_ms / 1000.0 if timeout_ms > 0 else None)
                    
                    all_results = {}
                    if was_notified:
                        with datastore.lock:
                            for key in keys:
                                start_id = resolved_start_ids[key]
                                key_results = []
                                item = datastore.get_item(key)
                                if item and item[0] == 'stream':
                                    for entry in item[1]:
                                        if parse_id(entry[0].decode()) > start_id:
                                            key_results.append(entry)
                                if key_results:
                                    all_results[key] = key_results
                    
                    client_socket.sendall(protocol.format_xread_response(all_results))
                else:
                    client_socket.sendall(response)
            else:
                response = handle_command(parts, datastore, server_state)
                if isinstance(response, tuple):
                    response_bytes, action = response
                    client_socket.sendall(response_bytes)
                    if action == "SEND_RDB_FILE":
                        rdb_response = f"${len(EMPTY_RDB_CONTENT)}\r\n".encode() + EMPTY_RDB_CONTENT
                        client_socket.sendall(rdb_response)
                        if server_state["role"] == "master":
                            server_state["replicas"].append(client_socket)
                else:
                    client_socket.sendall(response)
                    if command_name in WRITE_COMMANDS and server_state["role"] == "master":
                        for replica_socket in server_state["replicas"]:
                            replica_socket.sendall(request_bytes)

        except (IndexError, ConnectionResetError, ValueError):
            break
    
    print(f"Closing connection from {client_address}")
    if client_socket in server_state.get("replicas", []):
        server_state["replicas"].remove(client_socket)
    client_socket.close()

def connect_to_master(server_state, replica_port, datastore):
    master_host, master_port = server_state["master_host"], server_state["master_port"]
    try:
        master_socket = socket.create_connection((master_host, master_port))
        print(f"Connected to master at {master_host}:{master_port}")
        master_socket.sendall(protocol.format_array([protocol.format_bulk_string(b"PING")]))
        master_socket.recv(1024)
        master_socket.sendall(protocol.format_array([
            protocol.format_bulk_string(b"REPLCONF"),
            protocol.format_bulk_string(b"listening-port"),
            protocol.format_bulk_string(str(replica_port).encode())]))
        master_socket.recv(1024)
        master_socket.sendall(protocol.format_array([
            protocol.format_bulk_string(b"REPLCONF"),
            protocol.format_bulk_string(b"capa"),
            protocol.format_bulk_string(b"psync2")]))
        master_socket.recv(1024)
        master_socket.sendall(protocol.format_array([
            protocol.format_bulk_string(b"PSYNC"),
            protocol.format_bulk_string(b"?"),
            protocol.format_bulk_string(b"-1")]))

        while True:
            propagated_command = master_socket.recv(1024)
            if not propagated_command:
                break
            parts = propagated_command.strip().split(b'\r\n')
            handle_command(parts, datastore, server_state)
            
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