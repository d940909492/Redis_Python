import time
from . import protocol

def handle_ping(parts):
    return protocol.format_simple_string("PONG")

def handle_echo(parts):
    message = parts[4]
    return protocol.format_bulk_string(message)

def handle_set(parts, datastore):
    key = parts[4]
    value = parts[6]
    expiry_ms = None
    if len(parts) > 8 and parts[8].decode().upper() == 'PX':
        expiry_ms = int(parts[10].decode())
    
    datastore.set_item(key, ('string', (value, expiry_ms)))
    return protocol.format_simple_string("OK")

def handle_get(parts, datastore):
    key = parts[4]
    item = datastore.get_item(key)
    if not item or item[0] != 'string':
        return protocol.format_bulk_string(None)
    
    value, _ = item[1]
    return protocol.format_bulk_string(value)

def handle_type(parts, datastore):
    key = parts[4]
    item = datastore.get_item(key)
    type_name = "none"
    if item:
        type_name = item[0]
    return protocol.format_simple_string(type_name)


# use for create function or feature

COMMAND_HANDLERS = {
    "PING": handle_ping,
    "ECHO": handle_echo,
    "SET": handle_set,
    "GET": handle_get,
    "TYPE": handle_type,
}

def handle_command(parts, datastore):
    command_name = parts[2].decode().upper()
    handler = COMMAND_HANDLERS.get(command_name)

    if not handler:
        return protocol.format_error(f"unknown command '{command_name}'")
    
    if command_name in ["SET", "GET", "TYPE", "INCR", "XADD", "XRANGE", "XREAD"]:
        return handler(parts, datastore)
    else:
        return handler(parts)