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
                
                if entry_id_str == '*':
                    ms_time = 0
                    seq_num = 0
                    with GLOBAL_LOCK:
                        ms_time = int(time.time() * 1000)
                        
                        stored_item = DATA_STORE.get(key)
                        if stored_item and stored_item[0] == 'stream' and stored_item[1]:
                            last_entry_id_str = stored_item[1][-1][0].decode()
                            last_ms_time, last_seq_num = map(int, last_entry_id_str.split('-'))
                            
                            if ms_time <= last_ms_time:
                                ms_time = last_ms_time
                                seq_num = last_seq_num + 1
                        
                        generated_id_str = f"{ms_time}-{seq_num}"
                        generated_id_bytes = generated_id_str.encode()

                        entry_data = {}
                        field_value_parts = parts[8::2]
                        for i in range(0, len(field_value_parts), 2):
                            entry_data[field_value_parts[i]] = field_value_parts[i+1]
                        
                        new_entry = (generated_id_bytes, entry_data)
                        
                        if stored_item and stored_item[0] == 'stream':
                            stored_item[1].append(new_entry)
                        else:
                            DATA_STORE[key] = ('stream', [new_entry])

                    response = f"${len(generated_id_bytes)}\r\n".encode() + generated_id_bytes + b"\r\n"
                    client_socket.sendall(response)
                
                elif entry_id_str.endswith('-*'):
                    ms_time_str = entry_id_str[:-2]
                    try:
                        ms_time = int(ms_time_str)
                    except ValueError:
                        client_socket.sendall(b"-ERR Invalid stream ID specified\r\n")
                        continue
                    
                    seq_num = 0
                    with GLOBAL_LOCK:
                        stored_item = DATA_STORE.get(key)
                        if stored_item and stored_item[0] == 'stream' and stored_item[1]:
                            stream_entries = stored_item[1]
                            last_entry_id_str = stream_entries[-1][0].decode()
                            last_ms_time, last_seq_num = map(int, last_entry_id_str.split('-'))

                            if ms_time < last_ms_time:
                                client_socket.sendall(b"-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n")
                                continue
                            
                            if ms_time == last_ms_time:
                                seq_num = last_seq_num + 1
                        
                        if ms_time == 0 and seq_num == 0:
                            seq_num = 1

                        generated_id_str = f"{ms_time}-{seq_num}"
                        generated_id_bytes = generated_id_str.encode()

                        entry_data = {}
                        field_value_parts = parts[8::2]
                        for i in range(0, len(field_value_parts), 2):
                            entry_data[field_value_parts[i]] = field_value_parts[i+1]
                        
                        new_entry = (generated_id_bytes, entry_data)
                        
                        if stored_item and stored_item[0] == 'stream':
                            stored_item[1].append(new_entry)
                        else:
                            DATA_STORE[key] = ('stream', [new_entry])

                    response = f"${len(generated_id_bytes)}\r\n".encode() + generated_id_bytes + b"\r\n"
                    client_socket.sendall(response)
                
                else:
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
                        
                        if stored_item and stored_item[0] == 'stream' and stored_item[1]:
                            stream_entries = stored_item[1]
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
            
            elif command == "XREAD":
                try:
                    streams_keyword_idx = -1
                    for i, part in enumerate(parts):
                        if part.lower() == b'streams':
                            streams_keyword_idx = i
                            break
                    if streams_keyword_idx == -1:
                        raise ValueError("streams keyword not found")

                    num_args_after_streams = len(parts) - streams_keyword_idx - 1
                    num_streams = num_args_after_streams // 4

                    keys_start_idx = streams_keyword_idx + 2
                    ids_start_idx = keys_start_idx + (num_streams * 2)

                    keys = parts[keys_start_idx : ids_start_idx : 2]
                    start_ids_str = [p.decode() for p in parts[ids_start_idx::2]]

                    if len(keys) != len(start_ids_str):
                        raise ValueError("Mismatch between keys and IDs")
                except (ValueError, IndexError):
                    client_socket.sendall(b"-ERR syntax error\r\n")
                    continue
                
                def parse_complete_id(id_str):
                    ms, seq = map(int, id_str.split('-'))
                    return (ms, seq)

                all_results = {}
                with GLOBAL_LOCK:
                    for i in range(num_streams):
                        key = keys[i]
                        start_id = parse_complete_id(start_ids_str[i])
                        
                        key_results = []
                        stored_item = DATA_STORE.get(key)
                        if stored_item and stored_item[0] == 'stream':
                            for entry_id_bytes, entry_data in stored_item[1]:
                                entry_id_tuple = parse_complete_id(entry_id_bytes.decode())
                                if entry_id_tuple > start_id:
                                    key_results.append((entry_id_bytes, entry_data))
                        
                        if key_results:
                            all_results[key] = key_results
                
                if not all_results:
                    client_socket.sendall(b"$-1\r\n")
                else:
                    response_parts = [f"*{len(all_results)}\r\n".encode()]
                    for key, results in all_results.items():
                        response_parts.append(b"*2\r\n")
                        response_parts.append(f"${len(key)}\r\n".encode() + key + b"\r\n")
                        response_parts.append(f"*{len(results)}\r\n".encode())
                        for entry_id_bytes, entry_data in results:
                            response_parts.append(b'*2\r\n')
                            response_parts.append(f"${len(entry_id_bytes)}\r\n".encode() + entry_id_bytes + b"\r\n")
                            flat_kv = [item for pair in entry_data.items() for item in pair]
                            response_parts.append(f"*{len(flat_kv)}\r\n".encode())
                            for item in flat_kv:
                                response_parts.append(f"${len(item)}\r\n".encode() + item + b"\r\n")
                    client_socket.sendall(b"".join(response_parts))

            elif command == "XRANGE":
                key = parts[4]
                start_id_str = parts[6].decode()
                end_id_str = parts[8].decode()

                def parse_range_id(id_str, is_end_id=False):
                    if id_str == '-': return (0, 0)
                    if id_str == '+': return (float('inf'), float('inf'))
                    if '-' in id_str:
                        ms, seq = map(int, id_str.split('-'))
                        return (ms, seq)
                    else:
                        ms = int(id_str)
                        return (ms, float('inf') if is_end_id else 0)

                start_id = parse_range_id(start_id_str)
                end_id = parse_range_id(end_id_str, is_end_id=True)
                
                results = []
                with GLOBAL_LOCK:
                    stored_item = DATA_STORE.get(key)
                    if stored_item and stored_item[0] == 'stream':
                        stream_entries = stored_item[1]
                        for entry_id_bytes, entry_data in stream_entries:
                            entry_id_tuple = parse_range_id(entry_id_bytes.decode())
                            if start_id <= entry_id_tuple <= end_id:
                                results.append((entry_id_bytes, entry_data))

                if not results:
                    client_socket.sendall(b"*0\r\n")
                else:
                    response_parts = [f"*{len(results)}\r\n".encode()]
                    for entry_id_bytes, entry_data in results:
                        response_parts.append(b'*2\r\n')
                        response_parts.append(f"${len(entry_id_bytes)}\r\n".encode() + entry_id_bytes + b"\r\n")
                        
                        flat_kv = [item for pair in entry_data.items() for item in pair]
                        response_parts.append(f"*{len(flat_kv)}\r\n".encode())
                        for item in flat_kv:
                            response_parts.append(f"${len(item)}\r\n".encode() + item + b"\r\n")
                    
                    client_socket.sendall(b"".join(response_parts))

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