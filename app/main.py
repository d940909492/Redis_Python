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
                            response = b""
                            if cmd == "SET":
                                key, value = queued_parts[4], queued_parts[6]
                                DATA_STORE[key] = ('string', (value, None))
                                response = b"+OK\r\n"
                            elif cmd == "GET":
                                key = queued_parts[4]
                                item = DATA_STORE.get(key)
                                if not item or item[0] != 'string':
                                    response = b"$-1\r\n"
                                else:
                                    value, expiry = item[1]
                                    response = f"${len(value)}\r\n".encode() + value + b"\r\n"
                            elif cmd == "INCR":
                                key = queued_parts[4]
                                item = DATA_STORE.get(key)
                                if item is None:
                                    DATA_STORE[key] = ('string', (b'1', None))
                                    response = b":1\r\n"
                                elif item[0] == 'string':
                                    try:
                                        val = int(item[1][0].decode()) + 1
                                        DATA_STORE[key] = ('string', (str(val).encode(), item[1][1]))
                                        response = f":{val}\r\n".encode()
                                    except ValueError:
                                        response = b"-ERR value is not an integer or out of range\r\n"
                                else:
                                    response = b"-ERR WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
                            responses.append(response)
                    
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
            
            elif command == "XADD":
                key = parts[4]
                entry_id_str = parts[6].decode()
                response = b""
                with GLOBAL_LOCK:
                    entry_id_bytes_to_store = b''
                    if entry_id_str == '*':
                        ms_time = int(time.time() * 1000)
                        seq_num = 0
                        stored_item = DATA_STORE.get(key)
                        if stored_item and stored_item[0] == 'stream' and stored_item[1]:
                            last_ms, last_seq = map(int, stored_item[1][-1][0].decode().split('-'))
                            if ms_time <= last_ms:
                                ms_time = last_ms
                                seq_num = last_seq + 1
                        generated_id_str = f"{ms_time}-{seq_num}"
                        entry_id_bytes_to_store = generated_id_str.encode()
                    elif entry_id_str.endswith('-*'):
                        ms_time = int(entry_id_str.split('-')[0])
                        seq_num = 0
                        stored_item = DATA_STORE.get(key)
                        if stored_item and stored_item[0] == 'stream' and stored_item[1]:
                            last_ms, last_seq = map(int, stored_item[1][-1][0].decode().split('-'))
                            if ms_time < last_ms:
                                client_socket.sendall(b"-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n")
                                continue
                            if ms_time == last_ms:
                                seq_num = last_seq + 1
                        if ms_time == 0 and seq_num == 0:
                            seq_num = 1
                        generated_id_str = f"{ms_time}-{seq_num}"
                        entry_id_bytes_to_store = generated_id_str.encode()
                    else:
                        ms_time, seq_num = map(int, entry_id_str.split('-'))
                        if ms_time == 0 and seq_num == 0:
                            client_socket.sendall(b"-ERR The ID specified in XADD must be greater than 0-0\r\n")
                            continue
                        stored_item = DATA_STORE.get(key)
                        if stored_item and stored_item[0] == 'stream' and stored_item[1]:
                            last_ms, last_seq = map(int, stored_item[1][-1][0].decode().split('-'))
                            if ms_time < last_ms or (ms_time == last_ms and seq_num <= last_seq):
                                client_socket.sendall(b"-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n")
                                continue
                        entry_id_bytes_to_store = parts[6]
                    response = f"${len(entry_id_bytes_to_store)}\r\n".encode() + entry_id_bytes_to_store + b"\r\n"
                    entry_data = {}
                    field_value_parts = parts[8::2]
                    for i in range(0, len(field_value_parts), 2):
                        field = field_value_parts[i]
                        value = field_value_parts[i+1]
                        entry_data[field] = value
                    new_entry = (entry_id_bytes_to_store, entry_data)
                    stored_item = DATA_STORE.get(key)
                    if stored_item and stored_item[0] == 'stream':
                        stored_item[1].append(new_entry)
                    else:
                        DATA_STORE[key] = ('stream', [new_entry])
                    if key in BLOCKING_CONDITIONS:
                        BLOCKING_CONDITIONS[key].notify_all()
                client_socket.sendall(response)

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

            elif command == "XREAD":
                is_blocking = False
                timeout_ms = 0
                try:
                    block_keyword_idx = -1
                    for i, p in enumerate(parts):
                        if p.lower() == b'block':
                            block_keyword_idx = i
                            break
                    if block_keyword_idx != -1:
                        is_blocking = True
                        timeout_ms = int(parts[block_keyword_idx + 2].decode())
                    streams_keyword_idx = -1
                    for i, p in enumerate(parts):
                        if p.lower() == b'streams':
                            streams_keyword_idx = i
                            break
                    num_args_after_streams = len(parts) - streams_keyword_idx - 1
                    num_keys = num_args_after_streams // 4
                    keys_start_idx = streams_keyword_idx + 2
                    ids_start_idx = keys_start_idx + (num_keys * 2)
                    keys = parts[keys_start_idx:ids_start_idx:2]
                    start_ids_str = [p.decode() for p in parts[ids_start_idx::2]]
                except (ValueError, IndexError):
                    client_socket.sendall(b"-ERR syntax error\r\n")
                    continue
                def parse_id(id_str):
                    if id_str == '$': return '$'
                    ms, seq = map(int, id_str.split('-'))
                    return (ms, seq)
                def find_entries(key, start_id):
                    results = []
                    _stored_item = DATA_STORE.get(key)
                    if _stored_item and _stored_item[0] == 'stream':
                        for entry_id_bytes, entry_data in _stored_item[1]:
                            entry_id_tuple = parse_id(entry_id_bytes.decode())
                            if entry_id_tuple > start_id:
                                results.append((entry_id_bytes, entry_data))
                    return results
                all_results = {}
                resolved_start_ids = {}
                with GLOBAL_LOCK:
                    for i in range(num_keys):
                        key = keys[i]
                        start_id_val = start_ids_str[i]
                        if start_id_val == '$':
                            stored = DATA_STORE.get(key)
                            if stored and stored[0] == 'stream' and stored[1]:
                                start_id = parse_id(stored[1][-1][0].decode())
                            else:
                                start_id = (0, 0)
                        else:
                            start_id = parse_id(start_id_val)
                        resolved_start_ids[key] = start_id
                        key_results = find_entries(key, start_id)
                        if key_results:
                            all_results[key] = key_results
                if all_results or not is_blocking:
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
                else: 
                    with GLOBAL_LOCK:
                        block_key = keys[0]
                        if block_key not in BLOCKING_CONDITIONS:
                            BLOCKING_CONDITIONS[block_key] = threading.Condition(GLOBAL_LOCK)
                        condition = BLOCKING_CONDITIONS[block_key]
                        timeout_sec = timeout_ms / 1000.0 if timeout_ms > 0 else None
                        was_notified = condition.wait(timeout=timeout_sec)
                        if was_notified:
                            for i in range(num_keys):
                                key = keys[i]
                                start_id = resolved_start_ids[key]
                                key_results = find_entries(key, start_id)
                                if key_results:
                                    all_results[key] = key_results
                        if not condition._waiters:
                            del BLOCKING_CONDITIONS[block_key]
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