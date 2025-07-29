import socket
import threading

DATA_STORE = {}

def handle_client(client_socket, client_address):
    print(f"Connect from {client_address}")
    while True:
        try:
            request_bytes = client_socket.recv(1024)
            if not request_bytes:
                break

            # resp parsing
            parts = request_bytes.strip().split(b'\r\n')
            command = parts[2].decode().upper()

            # command handling
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
                DATA_STORE[key] = value
                client_socket.sendall(b"+OK\r\n")

            elif command == "GET":
                key = parts[4]
                value = DATA_STORE.get(key)
                if value:
                    response = f"${len(value)}\r\n".encode() + value + b"\r\n"
                    client_socket.sendall(response)
                else:
                    client_socket.sendall(b"$-1\r\n")
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