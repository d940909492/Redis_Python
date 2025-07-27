import socket
import threading


def handle_client(client_socket, client_address):
    print(f"New connection from {client_address}")
    try:
        while True:
            request = client_socket.recv(512)
            if not request:  # Client disconnected
                break
                
            data = request.decode()
            print(f"Received from {client_address}: {data.strip()}")
            
            if "ping" in data.lower():
                response = "+PONG\r\n"
                client_socket.send(response.encode())
    except (ConnectionResetError, BrokenPipeError):
        print(f"Client {client_address} disconnected")
    finally:
        client_socket.close()


def main():
    print("Redis server starting...")
    
    # Create server socket without reuse_port for Windows compatibility
    server_socket = socket.create_server(("localhost", 6379))
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    print("Server listening on localhost:6379")
    
    try:
        while True:
            client_socket, client_address = server_socket.accept()
            # Start a new thread for each client
            client_thread = threading.Thread(
                target=handle_client,
                args=(client_socket, client_address),
                daemon=True
            )
            client_thread.start()
    except KeyboardInterrupt:
        print("\nShutting down server...")
    finally:
        server_socket.close()


if __name__ == "__main__":
    main()