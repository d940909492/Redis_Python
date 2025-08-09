import time
from . import protocol

def handle_ping(parts, datastore, server_state):
    return protocol.format_simple_string("PONG")

def handle_echo(parts, datastore, server_state):
    return protocol.format_bulk_string(parts[4])

def handle_info(parts, datastore, server_state):
    section = parts[4].decode().lower()
    if section == "replication":
        role = server_state["role"]
        response_str = f"role:{role}"
        return protocol.format_bulk_string(response_str.encode())
    return protocol.format_bulk_string(b"")

def handle_set(parts, datastore, server_state):
    key, value = parts[4], parts[6]
    expiry_ms = None
    if len(parts) > 8 and parts[8].decode().upper() == 'PX':
        expiry_duration_ms = int(parts[10].decode())
        expiry_ms = int(time.time() * 1000) + expiry_duration_ms
    with datastore.lock:
        datastore.set_item(key, ('string', (value, expiry_ms)))
    return protocol.format_simple_string("OK")

def handle_get(parts, datastore, server_state):
    key = parts[4]
    with datastore.lock:
        item = datastore.get_item(key)
    if not item or item[0] != 'string': return protocol.format_bulk_string(None)
    value, _ = item[1]
    return protocol.format_bulk_string(value)

def handle_incr(parts, datastore, server_state):
    key = parts[4]
    with datastore.lock:
        item = datastore.get_item(key)
        if item is None:
            new_value = 1
            datastore.set_item(key, ('string', (b'1', None)))
            return protocol.format_integer(new_value)
        if item[0] != 'string':
            return protocol.format_error("WRONGTYPE Operation against a key holding the wrong kind of value")
        try:
            current_value = int(item[1][0].decode())
            new_value = current_value + 1
            datastore.set_item(key, ('string', (str(new_value).encode(), item[1][1])))
            return protocol.format_integer(new_value)
        except ValueError:
            return protocol.format_error("value is not an integer or out of range")

def handle_type(parts, datastore, server_state):
    key = parts[4]
    with datastore.lock:
        item = datastore.get_item(key)
    type_name = "none"
    if item: type_name = item[0]
    return protocol.format_simple_string(type_name)

def handle_lpush(parts, datastore, server_state):
    key = parts[4]
    elements = parts[6::2]
    with datastore.lock:
        item = datastore.get_item(key)
        current_list = item[1] if item and item[0] == 'list' else []
        elements.reverse()
        current_list[:0] = elements
        if not item or item[0] != 'list': datastore.set_item(key, ('list', current_list))
        datastore.notify_waiters(key)
    return protocol.format_integer(len(current_list))

def handle_rpush(parts, datastore, server_state):
    key = parts[4]
    elements = parts[6::2]
    with datastore.lock:
        item = datastore.get_item(key)
        current_list = item[1] if item and item[0] == 'list' else []
        current_list.extend(elements)
        if not item or item[0] != 'list': datastore.set_item(key, ('list', current_list))
        datastore.notify_waiters(key)
    return protocol.format_integer(len(current_list))

def handle_lpop(parts, datastore, server_state):
    key = parts[4]
    count_provided = len(parts) > 5
    with datastore.lock:
        item = datastore.get_item(key)
        if not item or item[0] != 'list' or not item[1]:
            return protocol.format_array([]) if count_provided else protocol.format_bulk_string(None)
        the_list = item[1]
        if count_provided:
            count = int(parts[6].decode())
            pop_count = min(count, len(the_list))
            elements_to_return = the_list[:pop_count]
            datastore.set_item(key, ('list', the_list[pop_count:]))
            return protocol.format_array([protocol.format_bulk_string(el) for el in elements_to_return])
        else:
            return protocol.format_bulk_string(the_list.pop(0))

def handle_llen(parts, datastore, server_state):
    key = parts[4]
    with datastore.lock:
        item = datastore.get_item(key)
        if not item or item[0] != 'list':
            return protocol.format_integer(0)
        return protocol.format_integer(len(item[1]))

def handle_lrange(parts, datastore, server_state):
    key = parts[4]
    start, end = int(parts[6].decode()), int(parts[8].decode())
    with datastore.lock:
        item = datastore.get_item(key)
        if not item or item[0] != 'list':
            return protocol.format_array([])
        the_list = item[1]
        sub_list = the_list[start:] if end == -1 else the_list[start:end+1]
        return protocol.format_array([protocol.format_bulk_string(i) for i in sub_list])

def handle_xadd(parts, datastore, server_state):
    key = parts[4]
    entry_id_str = parts[6].decode()
    with datastore.lock:
        entry_id_bytes_to_store = b''
        if entry_id_str == '*':
            ms_time = int(time.time() * 1000)
            seq_num = 0
            item = datastore.get_item(key)
            if item and item[0] == 'stream' and item[1]:
                last_ms, last_seq = map(int, item[1][-1][0].decode().split('-'))
                if ms_time <= last_ms:
                    ms_time = last_ms
                    seq_num = last_seq + 1
            entry_id_bytes_to_store = f"{ms_time}-{seq_num}".encode()
        elif entry_id_str.endswith('-*'):
            ms_time = int(entry_id_str.split('-')[0])
            seq_num = 0
            item = datastore.get_item(key)
            if item and item[0] == 'stream' and item[1]:
                last_ms, last_seq = map(int, item[1][-1][0].decode().split('-'))
                if ms_time < last_ms: return protocol.format_error("The ID specified in XADD is equal or smaller than the target stream top item")
                if ms_time == last_ms: seq_num = last_seq + 1
            if ms_time == 0 and seq_num == 0: seq_num = 1
            entry_id_bytes_to_store = f"{ms_time}-{seq_num}".encode()
        else:
            ms_time, seq_num = map(int, entry_id_str.split('-'))
            if ms_time == 0 and seq_num == 0: return protocol.format_error("The ID specified in XADD must be greater than 0-0")
            item = datastore.get_item(key)
            if item and item[0] == 'stream' and item[1]:
                last_ms, last_seq = map(int, item[1][-1][0].decode().split('-'))
                if ms_time < last_ms or (ms_time == last_ms and seq_num <= last_seq):
                    return protocol.format_error("The ID specified in XADD is equal or smaller than the target stream top item")
            entry_id_bytes_to_store = parts[6]
        
        field_value_parts = parts[8::2]
        entry_data = {field_value_parts[i]: field_value_parts[i+1] for i in range(0, len(field_value_parts), 2)}
        new_entry = (entry_id_bytes_to_store, entry_data)
        item = datastore.get_item(key)
        if item and item[0] == 'stream':
            item[1].append(new_entry)
        else:
            datastore.set_item(key, ('stream', [new_entry]))
        datastore.notify_waiters(key, notify_all=True)
    return protocol.format_bulk_string(entry_id_bytes_to_store)

def handle_xrange(parts, datastore, server_state):
    key = parts[4]
    start_id_str, end_id_str = parts[6].decode(), parts[8].decode()
    def parse_range_id(id_str, is_end_id=False):
        if id_str == '-': return (0, 0)
        if id_str == '+': return (float('inf'), float('inf'))
        if '-' in id_str: return tuple(map(int, id_str.split('-')))
        return (int(id_str), float('inf') if is_end_id else 0)
    start_id, end_id = parse_range_id(start_id_str), parse_range_id(end_id_str, True)
    results = []
    with datastore.lock:
        item = datastore.get_item(key)
        if item and item[0] == 'stream':
            for entry in item[1]:
                entry_id_tuple = parse_range_id(entry[0].decode())
                if start_id <= entry_id_tuple <= end_id:
                    results.append(entry)
    return protocol.format_stream_range_response(results)

def handle_xread(parts, datastore, server_state):
    try:
        streams_idx = parts.index(b'streams')
        num_keys = (len(parts) - streams_idx - 1) // 4
        keys_start, ids_start = streams_idx + 2, streams_idx + 2 + (num_keys * 2)
        keys = parts[keys_start:ids_start:2]
        start_ids_str = [p.decode() for p in parts[ids_start::2]]
    except (ValueError, IndexError):
        return protocol.format_error("syntax error")

    def parse_id(id_str):
        if id_str == '$': return '$'
        return tuple(map(int, id_str.split('-')))

    all_results = {}
    with datastore.lock:
        for i, key in enumerate(keys):
            id_val = start_ids_str[i]
            if id_val == '$':
                item = datastore.get_item(key)
                start_id = parse_id(item[1][-1][0].decode()) if item and item[1] else (0, 0)
            else:
                start_id = parse_id(id_val)
            key_results = []
            item = datastore.get_item(key)
            if item and item[0] == 'stream':
                for entry in item[1]:
                    if parse_id(entry[0].decode()) > start_id:
                        key_results.append(entry)
            if key_results:
                all_results[key] = key_results
    return protocol.format_xread_response(all_results)

COMMAND_HANDLERS = {
    "PING": handle_ping, "ECHO": handle_echo, "INFO": handle_info,
    "SET": handle_set, "GET": handle_get, "INCR": handle_incr,
    "TYPE": handle_type,
    "LPUSH": handle_lpush, "RPUSH": handle_rpush, "LPOP": handle_lpop,
    "LLEN": handle_llen, "LRANGE": handle_lrange,
    "XADD": handle_xadd, "XRANGE": handle_xrange, "XREAD": handle_xread,
}

def handle_command(parts, datastore, server_state):
    command_name = parts[2].decode().upper()
    handler = COMMAND_HANDLERS.get(command_name)
    if not handler:
        return protocol.format_error(f"unknown command '{command_name}'")
    return handler(parts, datastore, server_state)