import socket
import threading
import time

DATA_STORE = {}
GLOBAL_LOCK = threading.Lock()
BLOCKING_CONDITIONS = {}


def handle_client(client_socket, client_address):
    print(f"Connect from {client_address}")
    in_transaction = False
    transaction_queue = []

    while True:
        try:
            request_bytes = client_socket.recv(1024)
            if not request_bytes:
                break

            parts = request_bytes.strip().split(b'\r\n')
            command = parts[2].decode().upper()

            if in_transaction and command not in ["EXEC", "DISCARD", "MULTI"]:
                transaction_queue.append(parts)
                client_socket.sendall(b"+QUEUED\r\n")
                continue

            if command == "PING":
                client_socket.sendall(b"+PONG\r\n")

            elif command == "ECHO":
                message = parts[4]
                client_socket.sendall(f"${len(message)}\r\n".encode() + message + b"\r\n")

            elif command == "MULTI":
                in_transaction = True
                transaction_queue = []
                client_socket.sendall(b"+OK\r\n")
            
            elif command == "EXEC":
                if not in_transaction:
                    client_socket.sendall(b"-ERR EXEC without MULTI\r\n")
                else:
                    responses = []
                    with GLOBAL_LOCK:
                        for queued_parts in transaction_queue:
                            cmd = queued_parts[2].decode().upper()
                            if cmd == "SET":
                                key, value = queued_parts[4], queued_parts[6]
                                expiry = None
                                if len(queued_parts) > 8 and queued_parts[8].decode().upper() == 'PX':
                                    expiry = time.time() + (int(queued_parts[10].decode()) / 1000.0)
                                DATA_STORE[key] = ('string', (value, expiry))
                                responses.append(b"+OK\r\n")
                            elif cmd == "GET":
                                key = queued_parts[4]
                                item = DATA_STORE.get(key)
                                if not item or item[0] != 'string':
                                    responses.append(b"$-1\r\n")
                                else:
                                    value, expiry = item[1]
                                    if expiry is not None and time.time() > expiry:
                                        del DATA_STORE[key]
                                        responses.append(b"$-1\r\n")
                                    else:
                                        responses.append(f"${len(value)}\r\n".encode() + value + b"\r\n")
                            elif cmd == "INCR":
                                key = queued_parts[4]
                                item = DATA_STORE.get(key)
                                if item is None:
                                    new_val = 1
                                    DATA_STORE[key] = ('string', (b'1', None))
                                    responses.append(f":{new_val}\r\n".encode())
                                elif item[0] == 'string':
                                    try:
                                        val = int(item[1][0].decode()) + 1
                                        DATA_STORE[key] = ('string', (str(val).encode(), item[1][1]))
                                        responses.append(f":{val}\r\n".encode())
                                    except ValueError:
                                        responses.append(b"-ERR value is not an integer or out of range\r\n")
                                else:
                                    responses.append(b"-ERR WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
                            else:
                                responses.append(b"-ERR command not supported in transaction\r\n")

                    response_parts = [f"*{len(responses)}\r\n".encode()]
                    response_parts.extend(responses)
                    client_socket.sendall(b"".join(response_parts))

                    in_transaction = False
                    transaction_queue = []

            elif command == "TYPE":
                key = parts[4]
                type_name = "none"
                with GLOBAL_LOCK:
                    stored_item = DATA_STORE.get(key)
                    if stored_item:
                        type_name = stored_item[0]
                client_socket.sendall(f"+{type_name}\r\n".encode())

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

            elif command == "INCR":
                key = parts[4]
                response = b""
                with GLOBAL_LOCK:
                    stored_item = DATA_STORE.get(key)
                    if stored_item is None:
                        new_value = 1
                        DATA_STORE[key] = ('string', (b'1', None))
                        response = f":{new_value}\r\n".encode()
                    elif stored_item[0] == 'string':
                        value_bytes, expiry = stored_item[1]
                        try:
                            current_value = int(value_bytes.decode())
                            new_value = current_value + 1
                            new_value_bytes = str(new_value).encode()
                            DATA_STORE[key] = ('string', (new_value_bytes, expiry))
                            response = f":{new_value}\r\n".encode()
                        except ValueError:
                            response = b"-ERR value is not an integer or out of range\r\n"
                    else:
                        response = b"-ERR WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
                client_socket.sendall(response)
            
            
            else:
                client_socket.sendall(b"-ERR unknown command\r\n")

        except (IndexError, ConnectionResetError, ValueError):
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