import socket
import threading
import argparse
from .datastore import RedisDataStore
from .command_handler import handle_command
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
                response = handle_command(parts, datastore)
                if response == protocol.format_bulk_string(None):
                    try:
                        block_idx = parts.index(b'block')
                        timeout_ms = int(parts[block_idx + 2].decode())
                        streams_idx = parts.index(b'streams')
                        num_keys = (len(parts) - streams_idx - 1) // 4
                        keys_start, ids_start = streams_idx + 2, streams_idx + 2 + (num_keys * 2)
                        keys = parts[keys_start:ids_start:2]
                        start_ids_str = [p.decode() for p in parts[ids_start::2]]
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