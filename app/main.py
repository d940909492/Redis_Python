import socket
import threading
import argparse
from .datastore import RedisDataStore
from .command_handler import handle_command, COMMAND_HANDLERS
from . import protocol

def handle_client(client_socket, client_address, datastore):
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
                transaction_queue.append(parts)
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
                    for queued_parts in transaction_queue:
                        responses.append(handle_command(queued_parts, datastore))
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
                        response_arr = [protocol.format_bulk_string(key), protocol.format_bulk_string(popped)]
                        response = protocol.format_array(response_arr)
                    else:
                        condition = datastore.get_condition_for_key(key)
                        was_notified = condition.wait(timeout=None if timeout == 0 else timeout)
                        if was_notified:
                            item = datastore.get_item(key)
                            if item and item[0] == 'list' and item[1]:
                                popped = item[1].pop(0)
                                response_arr = [protocol.format_bulk_string(key), protocol.format_bulk_string(popped)]
                                response = protocol.format_array(response_arr)
                            else: response = protocol.format_bulk_string(None)
                        else: response = protocol.format_bulk_string(None)
                client_socket.sendall(response)
            elif command_name == "XREAD" and b'block' in [p.lower() for p in parts]:
                response = COMMAND_HANDLERS["XREAD"](parts, datastore)
                if response == protocol.format_bulk_string(None):
                    try:
                        streams_idx = parts.index(b'streams')
                        num_keys = (len(parts) - streams_idx - 1) // 4
                        keys_start_idx = streams_idx + 2
                        ids_start_idx = keys_start_idx + (num_keys * 2)
                        keys = parts[keys_start_idx:ids_start_idx:2]
                        start_ids_str = [p.decode() for p in parts[ids_start_idx::2]]
                        block_idx = parts.index(b'block')
                        timeout_ms = int(parts[block_idx + 2].decode())
                    except (ValueError, IndexError):
                        client_socket.sendall(protocol.format_error("syntax error"))
                        continue

                    all_results = {}
                    resolved_start_ids = {}

                    def parse_id(id_str):
                        if id_str == '$': return '$'
                        return tuple(map(int, id_str.split('-')))
                    
                    with datastore.lock:
                        for i in range(num_keys):
                            key = keys[i]
                            start_id_val = start_ids_str[i]
                            if start_id_val == '$':
                                stored = datastore.get_item(key)
                                start_id = parse_id(stored[1][-1][0].decode()) if stored and stored[1] else (0,0)
                            else:
                                start_id = parse_id(start_id_val)
                            resolved_start_ids[key] = start_id
                        
                        condition = datastore.get_condition_for_key(keys[0])
                        was_notified = condition.wait(timeout=timeout_ms / 1000.0 if timeout_ms > 0 else None)
                        
                        if was_notified:
                            for i in range(num_keys):
                                key = keys[i]
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
                response = handle_command(parts, datastore)
                client_socket.sendall(response)

        except (IndexError, ConnectionResetError, ValueError):
            break
    
    print(f"Closing connection from {client_address}")
    client_socket.close()

def main():
    print("Redis server start...")
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=6379)
    args = parser.parse_args()
    
    datastore = RedisDataStore()
    
    server_socket = socket.create_server(("localhost", args.port))
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    print(f"Server listen on localhost {args.port}")
    
    while True:
        client_socket, client_address = server_socket.accept()
        client_thread = threading.Thread(
            target=handle_client,
            args=(client_socket, client_address, datastore),
            daemon=True
        )
        client_thread.start()

if __name__ == "__main__":
    main()