import socket
host = socket.gethostname()
import json
import time
port = 12345
import sys
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.connect((host, port))
broker_port = s.recv(1024).decode('utf-8')
s.close()
mesage={"topic":"topic1","message":"hello"}

while True:

    message=input("Enter the message")    
    broker=socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    broker.connect((host, int(broker_port)))
    broker.send("producer".encode('utf-8'))
    broker.send(sys.argv[1].encode('utf-8'))
    broker.send(message.encode('utf-8'))
    broker.close()
    time.sleep(5)



