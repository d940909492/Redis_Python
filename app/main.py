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

                    # --- FIX: Correctly parse field-value pairs ---
                    entry_data = {}
                    field_value_parts = parts[8::2] # The actual data is at every other index starting from 8
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

            elif command == "XREAD":
                is_blocking = False
                timeout_ms = 0
                
                try:
                    # Find optional 'block' keyword
                    block_keyword_idx = -1
                    for i, p in enumerate(parts):
                        if p.lower() == b'block':
                            block_keyword_idx = i
                            break
                    
                    if block_keyword_idx != -1:
                        is_blocking = True
                        timeout_ms = int(parts[block_keyword_idx + 2].decode())
                    
                    # Find mandatory 'streams' keyword
                    streams_keyword_idx = -1
                    for i, p in enumerate(parts):
                        if p.lower() == b'streams':
                            streams_keyword_idx = i
                            break

                    # Calculate number of streams and their positions
                    num_keys_and_ids = len(parts) - streams_keyword_idx - 1
                    num_keys = num_keys_and_ids // 4

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

                # --- Initial non-blocking read ---
                all_results = {}
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
                        
                        key_results = find_entries(key, start_id)
                        if key_results:
                            all_results[key] = key_results
                
                # --- Decide whether to block or respond immediately ---
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
                else: # --- Blocking Path ---
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
                                start_id = parse_id(start_ids_str[i])
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

            # ... all other commands (XRANGE, SET, GET, list commands) ...
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