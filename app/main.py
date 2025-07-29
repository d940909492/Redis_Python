import socket
import threading


def handle_client(client_socket, client_address):
    print(f"Connect from {client_address}")
    request = client_socket.recv(512) 
    
    if request:
        data = request.decode().strip().split('\r\n')
        
        if len(data) >= 5 and data[0] == '*2' and data[2].lower() == '$4' and data[3].lower() == 'echo':
            arg_length = int(data[4][1:])
            arg = data[5]
            response = f"${arg_length}\r\n{arg}\r\n"
        else:
            response = "+PONG\r\n"
            
        client_socket.send(response.encode())


def main():
    print("Redis server start...")

    server_socket = socket.create_server(("localhost", 6379))
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    print("Server listen on localhost 6379")
    
    while True:
        client_socket, client_address = server_socket.accept()

        # Start a new thread for each client
        client_thread = threading.Thread(target=handle_client,
        args=(client_socket, client_address),daemon=True)
        client_thread.start()


if __name__ == "__main__":
    main()