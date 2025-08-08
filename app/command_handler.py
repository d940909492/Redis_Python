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
    if item: type_name = item[0]
    return protocol.format_simple_string(type_name)

def handle_lpush(parts, datastore):
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

def handle_rpush(parts, datastore):
    key = parts[4]
    elements = parts[6::2]
    with datastore.lock:
        item = datastore.get_item(key)
        current_list = item[1] if item and item[0] == 'list' else []
        current_list.extend(elements)
        if not item or item[0] != 'list': datastore.set_item(key, ('list', current_list))
        datastore.notify_waiters(key)
    return protocol.format_integer(len(current_list))

def handle_lpop(parts, datastore):
    key = parts[4]
    count_provided = len(parts) > 5
    
    with datastore.lock:
        item = datastore.get_item(key)

        if not item or item[0] != 'list' or not item[1]:
            if count_provided:
                return protocol.format_array([])
            else:
                return protocol.format_bulk_string(None)

        the_list = item[1]

        if count_provided:
            count = int(parts[6].decode())
            pop_count = min(count, len(the_list))
            
            elements_to_return = the_list[:pop_count]
            datastore.set_item(key, ('list', the_list[pop_count:]))

            response_parts = [protocol.format_bulk_string(el) for el in elements_to_return]
            return protocol.format_array(response_parts)
        else:
            popped_element = the_list.pop(0)
            return protocol.format_bulk_string(popped_element)

def handle_llen(parts, datastore):
    key = parts[4]
    with datastore.lock:
        item = datastore.get_item(key)
        if not item or item[0] != 'list':
            return protocol.format_integer(0)
        return protocol.format_integer(len(item[1]))

def handle_lrange(parts, datastore):
    key = parts[4]
    start, end = int(parts[6].decode()), int(parts[8].decode())
    with datastore.lock:
        item = datastore.get_item(key)
        if not item or item[0] != 'list':
            return protocol.format_array([])
        
        the_list = item[1]
        sub_list = the_list[start:] if end == -1 else the_list[start:end+1]
        
        response_parts = [protocol.format_bulk_string(list_item) for list_item in sub_list]
        return protocol.format_array(response_parts)

COMMAND_HANDLERS = {
    "PING": handle_ping, "ECHO": handle_echo,
    "SET": handle_set, "GET": handle_get, "INCR": handle_incr,
    "TYPE": handle_type,
    "LPUSH": handle_lpush, "RPUSH": handle_rpush, "LPOP": handle_lpop,
    "LLEN": handle_llen, "LRANGE": handle_lrange,
    # "XADD": handle_xadd, "XRANGE": handle_xrange, "XREAD": handle_xread,
}

def handle_command(parts, datastore):
    command_name = parts[2].decode().upper()
    handler = COMMAND_HANDLERS.get(command_name)
    if not handler:
        return protocol.format_error(f"unknown command '{command_name}'")
    return handler(parts, datastore)