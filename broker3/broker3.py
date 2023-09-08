import socket
import threading
import json
import time
import os
import pickle
list = []
latest = []



def consumer_producer():
    
    global list
    global latest
    
    while True:
        try:
            file_writeto = 1
            host = socket.gethostname()
            port = 4003
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.bind((host, port))
            s.listen(5)

            while True:
                try:
                    conn, addr = s.accept()
                    print(addr)
                    message = conn.recv(8).decode('utf-8')
                    print("message", message)
                    if message == "producer":

                        topic = conn.recv(6).decode('utf-8')
                        data = conn.recv(1024).decode('utf-8')
                        latest.append(data)
                        directory = topic
                        parent_dir = "C:/Users/admin/Desktop/KAFKA/broker1"
                        parent_dir1 = "C:/Users/admin/Desktop/KAFKA/broker2"
                        parent_dir2 = "C:/Users/admin/Desktop/KAFKA/broker3"

                        path = os.path.join(parent_dir, topic)
                        path1 = os.path.join(parent_dir1, topic)
                        path2 = os.path.join(parent_dir2, topic)
                        if os.path.exists(path):
                            if file_writeto == 1:
                                file_12 = open(path+"/"+"two.txt", "a")
                                file_22 = open(path1+"/"+"two.txt", "a")
                                file_31 = open(path2+"/"+"two.txt", "a")
                                file_12.write(data+"\n")
                                file_22.write(data+"\n")
                                file_31.write(data+"\n")
                                file_12.close()
                                file_22.close()
                                file_31.close()
                                file_writeto = 2
                            elif file_writeto == 2:
                                file_13 = open(path+"/"+"three.txt", "a")
                                file_23 = open(path1+"/"+"three.txt", "a")
                                file_33 = open(path2+"/"+"three.txt", "a")
                                file_13.write(data+"\n")
                                file_23.write(data+"\n")
                                file_33.write(data+"\n")
                                file_13.close()
                                file_23.close()
                                file_33.close()

                                file_writeto = 111
                            elif file_writeto == 111:
                                file_11 = open(path+"/"+"one.txt", "a")
                                file_21 = open(path1+"/"+"one.txt", "a")
                                file_32 = open(path2+"/"+"one.txt", "a")
                                file_11.write(data+"\n")
                                file_21.write(data+"\n")
                                file_32.write(data+"\n")
                                file_11.close()
                                file_21.close()
                                file_32.close()
                                file_writeto = 1

                        else:

                            print("path does not exists")
                            os.mkdir(path)
                            os.mkdir(path1)
                            os.mkdir(path2)
                            file_11 = open(path+"/"+"one.txt", "w")
                            file_12 = open(path+"/"+"two.txt", "w")
                            file_13 = open(path+"/"+"three.txt", "w")
                            file_21 = open(path1+"/"+"one.txt", "w")
                            file_22 = open(path1+"/"+"two.txt", "w")
                            file_23 = open(path1+"/"+"three.txt", "w")
                            file_31 = open(path2+"/"+"one.txt", "w")
                            file_32 = open(path2+"/"+"two.txt", "w")
                            file_33 = open(path2+"/"+"three.txt", "w")
                            file_11.write(data+"\n")
                            file_21.write(data+"\n")
                            file_31.write(data+"\n")
                            file_11.close()
                            file_12.close()
                            file_13.close()
                            file_21.close()
                            file_22.close()
                            file_23.close()
                            file_31.close()
                            file_32.close()
                            file_33.close()
                            file_writeto = 1

                    # print(path)
                    # print(topic)

                    # print(data)

                        conn.close()
                    elif message == "consumer":
                        method = conn.recv(5).decode('utf-8')
                        print("method", method)
                        if method == "early":
                            topic = conn.recv(6).decode('utf-8')

                            print(topic)
                            parent_dir = "C:/Users/admin/Desktop/KAFKA/broker3"
                            path = os.path.join(parent_dir, topic)
                            if os.path.exists(path):
                                file_11 = open(path+"/"+"one.txt", "r")
                                file_12 = open(path+"/"+"two.txt", "r")
                                file_13 = open(path+"/"+"three.txt", "r")
                                print("file_11")
                                r1 = file_11.readline()

                                r2 = file_12.readline()
                                r3 = file_13.readline()
                                arr = []

                                while r1 != '' or r2 != '' or r3 != '':

                                    arr.append(r1)
                                    arr.append(r2)
                                    arr.append(r3)

                                    r1 = file_11.readline()

                                    r2 = file_12.readline()
                                    r3 = file_13.readline()
                                    print(arr)
                                data_string = pickle.dumps(arr)
                                conn.send(data_string)

                        # conn.send("over".encode('utf-8'))
                                conn.close()
                                print(latest)
                        elif method == "lates":
                            while True:
                                try:
                                    print(latest)

                                    data_strin = pickle.dumps(latest)
                                   

                                    conn.send(data_strin)  # type: ignore
                                    
                                except:
                                    print("error")
                                    continue

                except socket.error as err:
                    continue
        except:
            continue


if "__main__" == "__main__":

    threading._start_new_thread(consumer_producer, ())   # type: ignore
    while True:
        pass
