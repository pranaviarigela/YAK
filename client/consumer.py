import socket
host = socket.gethostname()
port = 12345
import pickle
import time
import sys
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.connect((host, port))
s.send("consumer".encode('utf-8'))
broker_port = s.recv(1024).decode('utf-8')
s.close()
broker=socket.socket(socket.AF_INET, socket.SOCK_STREAM)
broker.connect((host, int(broker_port)))
broker.send("consumer".encode('utf-8'))

broker.send(sys.argv[2].encode('utf-8'))
broker.send(sys.argv[1].encode('utf-8'))
# while True:
#     print(s.recv(1024).decode('utf-8'))
# while True:

if sys.argv[2]=="early":
    data=broker.recv(1024)

    data_arr=pickle.loads(data)
    for i in data_arr:
        print(i.strip('\n'))
    broker.close()
elif sys.argv[2]=="lates":
    while True:
        data=broker.recv(1024)

        data_arr=pickle.loads(data)
        for i in data_arr:
            print(i.strip())
        # broker.close()

    # print(data)
    # if(data=="over"):
    #     broker.close()
    #     break
    
    


    
    #time.sleep(5)
    


