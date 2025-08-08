import time
from . import protocol

def handle_ping(parts, datastore):
    return protocol.format_simple_string("PONG")

def handle_echo(parts, datastore):
    return protocol.format_bulk_string(parts[4])

def handle_set(parts, datastore):
    key, value = parts[4], parts[6]
    expiry_ms = None
    if len(parts) > 8 and parts[8].decode().upper() == 'PX':
        expiry_duration_ms = int(parts[10].decode())
        expiry_ms = int(time.time() * 1000) + expiry_duration_ms
    
    with datastore.lock:
        datastore.set_item(key, ('string', (value, expiry_ms)))
    
    return protocol.format_simple_string("OK")

def handle_get(parts, datastore):
    key = parts[4]
    with datastore.lock:
        item = datastore.get_item(key)
    
    if not item or item[0] != 'string':
        return protocol.format_bulk_string(None)
    
    value, _ = item[1]
    return protocol.format_bulk_string(value)

def handle_incr(parts, datastore):
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

def handle_type(parts, datastore):
    key = parts[4]
    with datastore.lock:
        item = datastore.get_item(key)
    
    type_name = "none"
    if item:
        type_name = item[0]
    return protocol.format_simple_string(type_name)

COMMAND_HANDLERS = {
    "PING": handle_ping,
    "ECHO": handle_echo,
    "SET": handle_set,
    "GET": handle_get,
    "TYPE": handle_type,
    "INCR": handle_incr,
}

def handle_command(parts, datastore):
    command_name = parts[2].decode().upper()
    handler = COMMAND_HANDLERS.get(command_name)

    if not handler:
        return protocol.format_error(f"unknown command '{command_name}'")
    
    return handler(parts, datastore)