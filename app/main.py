def handle_client(client_socket, client_address):
    in_transaction = False
    transaction_queue = []
    # ... rest of the loop