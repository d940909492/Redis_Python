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

                if stored_item and stored_item[0] == 'list':
                    current_list = stored_item[1]
                    current_list.extend(elements)
                    list_length = len(current_list)
                else:
                    DATA_STORE[key] = ('list', elements)
                    list_length = len(elements)

                response = f":{list_length}\r\n".encode()
                client_socket.sendall(response)

            elif command == "LPUSH":
                key = parts[4]
                elements = parts[6::2]

                stored_item = DATA_STORE.get(key)
                elements.reverse()

                if stored_item and stored_item[0] == 'list':
                    current_list = stored_item[1]
                    current_list[:0] = elements
                    list_length = len(current_list)
                else:
                    DATA_STORE[key] = ('list', elements)
                    list_length = len(elements)
                
                response = f":{list_length}\r\n".encode()
                client_socket.sendall(response)
            
            elif command == "LRANGE":
                key = parts[4]
                start = int(parts[6].decode())
                end = int(parts[8].decode())
                
                stored_item = DATA_STORE.get(key)
                
                if stored_item is None or stored_item[0] != 'list':
                    client_socket.sendall(b"*0\r\n")
                    continue
                
                the_list = stored_item[1]
                
                if end == -1:
                    sub_list = the_list[start:]
                else:
                    sub_list = the_list[start : end + 1]
                
                response_parts = [f"*{len(sub_list)}\r\n".encode()]
                for item in sub_list:
                    response_parts.append(f"${len(item)}\r\n".encode())
                    response_parts.append(item)
                    response_parts.append(b"\r\n")
                
                client_socket.sendall(b"".join(response_parts))

            elif command == "LLEN":
                key = parts[4]
                stored_item = DATA_STORE.get(key)
                list_length = 0

                if stored_item and stored_item[0] == 'list':
                    list_length = len(stored_item[1])
                
                response = f":{list_length}\r\n".encode()
                client_socket.sendall(response)

            elif command == "LPOP":
                key = parts[4]
                stored_item = DATA_STORE.get(key)

                # Return null if key doesn't exist, isn't list, or empty list
                if not stored_item or stored_item[0] != 'list' or not stored_item[1]:
                    client_socket.sendall(b"$-1\r\n")
                    continue
                
                the_list = stored_item[1]
                popped_element = the_list.pop(0)
                
                # Respond with the popped element as RESP bulk string
                response = f"${len(popped_element)}\r\n".encode() + popped_element + b"\r\n"
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