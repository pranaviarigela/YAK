import socket
import threading
import time
master_port=4001
def consumer(conn):
    global master_port
    while True:
        try:
            if(conn.recv(1024).decode('utf-8')=="consumer"):
                conn.send(str(master_port).encode('utf-8'))
        except socket.error as err:
            print("")
        time.sleep(5)
def producer(conn):
    global master_port
    while True:
        try:
            if(conn.recv(1024).decode('utf-8')=="producer"):
                conn.send(str(master_port).encode('utf-8'))
        except socket.error as err:
            print("")
        time.sleep(5)

# conn.close()
def broker1():
    global master_port
    while True:
        try:
            host = socket.gethostname()
            port = 4001
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect((host, port))
            print("b1",master_port)
            # conn.send("connected to b1".encode('utf-8'))
            # s_zookeper.send("connected to b1".encode('utf-8'))
            s.close()
        except socket.error as err:
            
            # conn.send("not connected to b1".encode('utf-8'))
            if master_port==4001:
                master_port=4002
        # s_zookeper.send("not connected to b1".encode('utf-8'))
        time.sleep(5)
def broker2():
    global master_port
    while True:
        try:
            host = socket.gethostname()
            port = 4002
            b= socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            
            b.connect((host, port))
            print("b2",master_port)
            # conn.send("connected to b2".encode('utf-8'))
            
            # s_zookeper.send("connected to b2".encode('utf-8'))
            b.close()
        except socket.error as err:
            
            # conn.send("not connected to b2".encode('utf-8'))
            if master_port==4002:
                master_port=4003
            # s_zookeper.send("not connected to b2".encode('utf-8'))
        time.sleep(5)
def broker3():
    global master_port
    while True:
        try:
            host = socket.gethostname()
            port = 4003
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect((host, port))
            print("b3",master_port)
            # conn.send("connected to b3".encode('utf-8'))
            # s_zookeper.send("connected to b3".encode('utf-8'))
            
            s.close()
        except socket.error as err:
            # conn.send("not connected to b3".encode('utf-8'))
            
            if master_port==4003:
                master_port=4001
            # s_zookeper.send("not connected to b3".encode('utf-8'))
        time.sleep(5)
def consumer_producer():
    
    global master_port
    while True:
        host = socket.gethostname()
        port = 12345
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.bind((host, port))
        s.listen(5)
    
        conn, addr = s.accept()
        conn.send(str(master_port).encode('utf-8'))
        conn.close()

if "__main__"=="__main__":

   
    threading._start_new_thread(broker1,()) 
    threading._start_new_thread(broker2,())
    threading._start_new_thread(broker3,()) 
    threading._start_new_thread(consumer_producer,()) 

    
    
          
        
    while True:
        pass
        
   
    