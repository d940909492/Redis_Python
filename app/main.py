import socket
import threading
import time
import collections


GLOBAL_LOCK = threading.Lock()

BLOCKING_CONDITIONS = {}

DATA_STORE = {}


def handle_client(client_socket, client_address):
    print(f"Connect from {client_address}")
    while True:
        try:
            request_bytes = client_socket.recv(1024)
            if not request_bytes:
                break

            parts = request_bytes.strip().split(b'\r\n')
            command = parts[2].decode().upper()

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

                with GLOBAL_LOCK:
                    DATA_STORE[key] = ('string', (value, expiry_timestamp))
                client_socket.sendall(b"+OK\r\n")

            elif command == "GET":
                key = parts[4]
                response = b"$-1\r\n"
                with GLOBAL_LOCK:
                    stored_item = DATA_STORE.get(key)
                    if stored_item and stored_item[0] == 'string':
                        _type, (value, expiry_timestamp) = stored_item
                        if expiry_timestamp is not None and time.time() > expiry_timestamp:
                            del DATA_STORE[key]
                        else:
                            response = f"${len(value)}\r\n".encode() + value + b"\r\n"
                client_socket.sendall(response)

            elif command == "RPUSH" or command == "LPUSH":
                key = parts[4]
                elements = parts[6::2]
                response = b""

                with GLOBAL_LOCK:
                    stored_item = DATA_STORE.get(key)
                    if stored_item and stored_item[0] == 'list':
                        current_list = stored_item[1]
                    else:
                        current_list = []
                        DATA_STORE[key] = ('list', current_list)
                    
                    if command == "RPUSH":
                        current_list.extend(elements)
                    else:
                        elements.reverse()
                        current_list[:0] = elements
                    
                    response = f":{len(current_list)}\r\n".encode()

                    if key in BLOCKING_CONDITIONS:
                        condition = BLOCKING_CONDITIONS[key]
                        condition.notify()

                client_socket.sendall(response)

            elif command == "BLPOP":
                key = parts[4]
                response = b""

                with GLOBAL_LOCK:
                    stored_item = DATA_STORE.get(key)
                    if stored_item and stored_item[0] == 'list' and stored_item[1]:
                        the_list = stored_item[1]
                        popped_element = the_list.pop(0)
                        
                        response_parts = [b"*2\r\n", f"${len(key)}\r\n".encode(), key, b"\r\n", f"${len(popped_element)}\r\n".encode(), popped_element, b"\r\n"]
                        response = b"".join(response_parts)
                    else:
                        if key not in BLOCKING_CONDITIONS:
                            BLOCKING_CONDITIONS[key] = threading.Condition(GLOBAL_LOCK)
                        condition = BLOCKING_CONDITIONS[key]
                        
                        condition.wait()

                        the_list = DATA_STORE.get(key)[1]
                        popped_element = the_list.pop(0)

                        if not condition._waiters:
                            del BLOCKING_CONDITIONS[key]

                        response_parts = [b"*2\r\n", f"${len(key)}\r\n".encode(), key, b"\r\n", f"${len(popped_element)}\r\n".encode(), popped_element, b"\r\n"]
                        response = b"".join(response_parts)
                
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