def TCPSocketLisenter(port, queue):
    global thread_stop_event

    while not thread_stop_event.is_set():
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5)
            sock.bind(('', port))
            sock.listen(1)
            conn, _ = sock.accept()
            data = conn.recv(1024)
            if data:
                queue.append(data)
        except socket.timeout:
            pass
        except Exception as e:
            print(e)
