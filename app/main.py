import socket
import threading
from .datastore import RedisDataStore
from .command_handler import handle_command

def handle_client(client_socket, client_address, datastore):
    print(f"Connect from {client_address}")
    in_transaction = False
    transaction_queue = []

    while True:
        try:
            request_bytes = client_socket.recv(1024)
            if not request_bytes:
                break

            parts = request_bytes.strip().split(b'\r\n')
            command_name = parts[2].decode().upper()

            if in_transaction and command_name not in ["EXEC", "DISCARD"]:
                transaction_queue.append(parts)
                client_socket.sendall(b"+QUEUED\r\n")
                continue

            if command_name == "MULTI":
                in_transaction = True
                transaction_queue = []
                client_socket.sendall(b"+OK\r\n")
            elif command_name == "EXEC":
                if not in_transaction:
                    client_socket.sendall(b"-ERR EXEC without MULTI\r\n")
                else:
                    responses = []
                    for queued_parts in transaction_queue:
                        response = handle_command(queued_parts, datastore)
                        responses.append(response)
                    final_response_parts = [f"*{len(responses)}\r\n".encode()]
                    final_response_parts.extend(responses)
                    client_socket.sendall(b"".join(final_response_parts))
                    in_transaction = False
                    transaction_queue = []
            else:
                response = handle_command(parts, datastore)
                client_socket.sendall(response)

        except (IndexError, ConnectionResetError, ValueError):
            break
    
    print(f"Closing connection from {client_address}")
    client_socket.close()

def main():
    print("Redis server start...")
    
    datastore = RedisDataStore()
    
    server_socket = socket.create_server(("localhost", 6379))
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    print("Server listen on localhost 6379")
    
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