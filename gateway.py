#!/usr/bin/python 

import serial
import time
import io
import Queue as queue
import paho.mqtt.client as mqtt

x=0
go=True

ser=None
sio=None
sConnected=False

def on_message(client, userdata, msg):
    q.put(msg)
    print 'CB: ', str(msg.payload)

def on_publish(client, userdata, mid):
    print 'OP: ', mid

def m2g(to, msg):
    print 'SWRITE:', to, msg

mqttc = mqtt.Client()
mqttc.on_message = on_message
mqttc.on_publish = on_publish

q = queue.Queue()
while go:
    try:
        ser = serial.Serial('/dev/ttyACM0', 115200, timeout=1)
        sio = io.TextIOWrapper(io.BufferedRWPair(ser, ser))
        print 'Connected to Serial'
        sConnected=True
        mqttc.connect('localhost', 1883)
        mqttc.subscribe('pmmg/in/#', qos=2)
        x=0
        while sConnected and x < 10:
            try:
                mqttc.loop_misc()
                print 'LM:'
                line = sio.readline().rstrip()
                if line != '':
                    #node_id, child_sensor_id, message_type, ack, sub_type, payload = line.split(';')
                    fields = line.split(';')
                    if len(fields) == 6:
                        topic='pmmg/out'
                        topic = topic + '/' + fields[0]
                        topic = topic + '/' + fields[1]
                        topic = topic + '/' + fields[2]
                        topic = topic + '/' + fields[3]
                        topic = topic + '/' + fields[4]
                        mqttc.publish(topic, fields[5], 2, True)
                    print 'SREAD: ', line
                x = x + 1
                mqttc.loop_write()
                print 'LW:'
                while True:
                    mqttc.loop_read()
                    print 'LR:'
                    try:
                       m = q.get(False)
                       m2g('fred', m.payload)
                       q.task_done()
                    except queue.Empty:
                       break

            except IOError as ioe:
                print 'IOError', ioe
            except serial.SerialException:
                print 'Device disconnected'
                sConnected=False
    except KeyboardInterrupt:
        print 'Quitting'
        go=False
    except OSError as ose:
        time.sleep(1)
        print 'No can connect', ose
    except serial.SerialException as e:
        print e
    finally:
        if ser and not ser.closed:
            ser.close()
            print 'Closed ser'
        mqttc.disconnect()
        print 'disc mqtt'

print 'bye'
