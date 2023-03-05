#!/usr/bin/env python3

import os
import socket

host = os.environ.get('BITCASK_HOST', '127.0.0.1')
port = os.environ.get('BITCASK_PORT', 6969)

for message in (
    'set\r\n3\r\nfoo\r\n3\r\nbar',
    'get\r\n3\r\nfoo',
):
    print(message, '\n')
    client_socket = socket.socket()
    client_socket.connect((host, port))
    client_socket.send(message.encode())
    data = client_socket.recv(1024).decode()
    client_socket.close()

print('Response: ' + data)
