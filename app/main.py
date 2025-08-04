import socket
import threading
import time

DATA_STORE = {}
GLOBAL_LOCK = threading.Lock()
BLOCKING_CONDITIONS = {}


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

            elif command == "TYPE":
                key = parts[4]
                type_name = "none"
                with GLOBAL_LOCK:
                    stored_item = DATA_STORE.get(key)
                    if stored_item:
                        type_name = stored_item[0]
                response = f"+{type_name}\r\n".encode()
                client_socket.sendall(response)
            
            elif command == "XADD":
                key = parts[4]
                entry_id_str = parts[6].decode()
                
                try:
                    ms_time, seq_num = map(int, entry_id_str.split('-'))
                except ValueError:
                    client_socket.sendall(b"-ERR Invalid stream ID specified\r\n")
                    continue

                if ms_time == 0 and seq_num == 0:
                    client_socket.sendall(b"-ERR The ID specified in XADD must be greater than 0-0\r\n")
                    continue

                with GLOBAL_LOCK:
                    stored_item = DATA_STORE.get(key)
                    
                    if stored_item and stored_item[0] == 'stream':
                        stream_entries = stored_item[1]
                        if stream_entries:
                            last_entry_id_str = stream_entries[-1][0].decode()
                            last_ms_time, last_seq_num = map(int, last_entry_id_str.split('-'))
                            
                            if ms_time < last_ms_time or (ms_time == last_ms_time and seq_num <= last_seq_num):
                                client_socket.sendall(b"-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n")
                                continue
                    
                    entry_data = {}
                    field_value_parts = parts[8::2]
                    for i in range(0, len(field_value_parts), 2):
                        field = field_value_parts[i]
                        value = field_value_parts[i+1]
                        entry_data[field] = value
                    
                    new_entry = (parts[6], entry_data)
                    
                    if stored_item and stored_item[0] == 'stream':
                        stream_entries.append(new_entry)
                    else:
                        DATA_STORE[key] = ('stream', [new_entry])

                response = f"${len(parts[6])}\r\n".encode() + parts[6] + b"\r\n"
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
                        BLOCKING_CONDITIONS[key].notify()

                client_socket.sendall(response)

            elif command == "LPOP":
                key = parts[4]
                response = b""
                with GLOBAL_LOCK:
                    count_provided = len(parts) > 5
                    if count_provided:
                        count = int(parts[6].decode())
                        stored_item = DATA_STORE.get(key)
                        if not stored_item or stored_item[0] != 'list' or not stored_item[1]:
                            response = b"*0\r\n"
                        else:
                            the_list = stored_item[1]
                            pop_count = min(count, len(the_list))
                            elements_to_return = the_list[:pop_count]
                            DATA_STORE[key] = ('list', the_list[pop_count:])
                            
                            response_parts = [f"*{len(elements_to_return)}\r\n".encode()]
                            for item in elements_to_return:
                                response_parts.append(f"${len(item)}\r\n".encode())
                                response_parts.append(item)
                                response_parts.append(b"\r\n")
                            response = b"".join(response_parts)
                    else:
                        stored_item = DATA_STORE.get(key)
                        if not stored_item or stored_item[0] != 'list' or not stored_item[1]:
                            response = b"$-1\r\n"
                        else:
                            the_list = stored_item[1]
                            popped_element = the_list.pop(0)
                            response = f"${len(popped_element)}\r\n".encode() + popped_element + b"\r\n"
                client_socket.sendall(response)
            
            elif command == "BLPOP":
                key = parts[4]
                timeout = float(parts[6].decode())
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
                        
                        wait_timeout = None if timeout == 0 else timeout
                        was_notified = condition.wait(timeout=wait_timeout)
                        
                        if was_notified:
                            the_list = DATA_STORE.get(key)[1]
                            popped_element = the_list.pop(0)
                            response_parts = [b"*2\r\n", f"${len(key)}\r\n".encode(), key, b"\r\n", f"${len(popped_element)}\r\n".encode(), popped_element, b"\r\n"]
                            response = b"".join(response_parts)
                        else:
                            response = b"$-1\r\n"

                        if not condition._waiters:
                            del BLOCKING_CONDITIONS[key]

                client_socket.sendall(response)

            elif command == "LRANGE" or command == "LLEN":
                 with GLOBAL_LOCK:
                    key = parts[4]
                    stored_item = DATA_STORE.get(key)
                    if command == "LLEN":
                        list_length = 0
                        if stored_item and stored_item[0] == 'list':
                            list_length = len(stored_item[1])
                        client_socket.sendall(f":{list_length}\r\n".encode())
                    else:
                        if stored_item is None or stored_item[0] != 'list':
                            client_socket.sendall(b"*0\r\n")
                        else:
                            start = int(parts[6].decode())
                            end = int(parts[8].decode())
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