def format_simple_string(s):
    return f"+{s}\r\n".encode()

def format_error(s):
    return f"-ERR {s}\r\n".encode()

def format_integer(i):
    return f":{i}\r\n".encode()

def format_bulk_string(b):
    if b is None:
        return b"$-1\r\n"
    return f"${len(b)}\r\n".encode() + b + b"\r\n"

def format_array(arr):
    if arr is None:
        return b"*-1\r\n"
    if not arr:
        return b"*0\r\n"
        
    response_parts = [f"*{len(arr)}\r\n".encode()]
    for item in arr:
        response_parts.append(item)
    return b"".join(response_parts)

def format_stream_range_response(entries):
    if not entries:
        return format_array([])
        
    response_parts = [f"*{len(entries)}\r\n".encode()]
    for entry_id_bytes, entry_data in entries:
        response_parts.append(b'*2\r\n')
        response_parts.append(format_bulk_string(entry_id_bytes))
        
        flat_kv_list = [item for pair in entry_data.items() for item in pair]
        response_parts.append(f"*{len(flat_kv_list)}\r\n".encode())
        for item in flat_kv_list:
            response_parts.append(format_bulk_string(item))
            
    return b"".join(response_parts)

def format_xread_response(results_dict):
    if not results_dict:
        return format_bulk_string(None)
    
    response_parts = [f"*{len(results_dict)}\r\n".encode()]
    for key, entries in results_dict.items():
        response_parts.append(b'*2\r\n')
        response_parts.append(format_bulk_string(key))
        response_parts.append(format_stream_range_response(entries))
    
    return b"".join(response_parts)