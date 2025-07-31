import socket
import threading
import time


DATA_STORE = {}


def handle_client(client_socket, client_address):
    print(f"Connect from {client_address}")
    while True:
        try:
            request_bytes = client_socket.recv(1024)
            if not request_bytes:
                break

            # RESP parsing
            parts = request_bytes.strip().split(b'\r\n')
            command = parts[2].decode().upper()

            # Command handling
            if command == "PING":
                response = b"+PONG\r\n"
                client_socket.sendall(response)

            elif command == "ECHO":
                message = parts[4]
                response = f"${len(message)}\r\n".encode() + message + b"\r\n"
                client_socket.sendall(response)

            elif command == "SET":
                key = parts[4]
                value = parts[6]
                expiry_timestamp = None
                
                if len(parts) > 8 and parts[8].decode().upper() == 'PX':
                    expiry_ms = int(parts[10].decode())
                    expiry_timestamp = time.time() + (expiry_ms / 1000.0)

                DATA_STORE[key] = ('string', (value, expiry_timestamp))
                client_socket.sendall(b"+OK\r\n")

            elif command == "GET":
                key = parts[4]
                stored_item = DATA_STORE.get(key)
                
                # Check for is key exist and correct type
                if stored_item is None or stored_item[0] != 'string':
                    client_socket.sendall(b"$-1\r\n")
                    continue

                _type, (value, expiry_timestamp) = stored_item

                if expiry_timestamp is not None and time.time() > expiry_timestamp:
                    del DATA_STORE[key]
                    client_socket.sendall(b"$-1\r\n")
                else:
                    response = f"${len(value)}\r\n".encode() + value + b"\r\n"
                    client_socket.sendall(response)
            
            elif command == "RPUSH":
                key = parts[4]
                elements = parts[6::2]

                stored_item = DATA_STORE.get(key)

                # Check if a list already exists for the key
                if stored_item and stored_item[0] == 'list':
                    current_list = stored_item[1]
                    current_list.extend(elements)
                    list_length = len(current_list)
                else:
                    DATA_STORE[key] = ('list', elements)
                    list_length = len(elements)

                # Respond with the new length of the list
                response = f":{list_length}\r\n".encode()
                client_socket.sendall(response)

            else:
                client_socket.sendall(b"-ERR unknown command\r\n")

        except (IndexError, ConnectionResetError):
            break
    
    print(f"Closing connection from {client_address}")
    client_socket.close()


def main():
    print("Redis server start...")
    
    server_socket = socket.create_server(("localhost", 6379))
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    print("Server listen on localhost 6379")
    
    while True:
        client_socket, client_address = server_socket.accept()
        
        client_thread = threading.Thread(
            target=handle_client,
            args=(client_socket, client_address),
            daemon=True
        )
        client_thread.start()


if __name__ == "__main__":
    main()