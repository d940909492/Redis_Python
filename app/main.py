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