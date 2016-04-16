#!/usr/bin/python 

import mtypes
import serial
import time
import io
import Queue as queue
import paho.mqtt.client as mqtt
import signal
import getopt
import sys

isInfo=True

def info(line):
    global isInfo
    if isInfo:
        print line

def debug(line):
    global isDebug
    if isDebug:
        print line

def sighndlr(signal, frame):
    global go
    print 'received signal %d' % (signal)
    go=False


def on_message(client, userdata, msg):
    q.put(msg)

def on_publish(client, userdata, mid):
    pass

def m2g(msg):
    tp = msg.topic.split('/')
    node_id=tp[2]
    child_id=tp[3]
    msg_type_s=tp[4]
    ack_s=tp[5]
    sub_type_s=tp[6]
    msg_type = mtypes.message_types[msg_type_s]
    if msg_type == mtypes.M_INTERNAL:
        sub_type = mtypes.internal_sub_types[sub_type_s]
    elif msg_type == mtypes.M_SET:
        sub_type = mtypes.set_sub_types[sub_type_s]
    elif msg_type == mtypes.M_PRESENTATION:
        sub_type = mtypes.presentation_sub_types[sub_type_s]
    else:
        print 'WARNING unknown message sub type %s' % sub_type_s

    print 'SWRITE: %d;%d;%d;%d;%d;%s' % (int(node_id), int(child_id), msg_type, int(ack_s), sub_type, msg.payload)

def g2m(line):
    debug(line)
    try:
        node_id, child_sensor_id, message_type, ack, sub_type, payload = line.split(';', 5)
        message_type = int(message_type)
        sub_type = int(sub_type)
        topic='pmmg/out'
        topic = topic + '/' + node_id
        topic = topic + '/' + child_sensor_id
        topic = topic + '/' + mtypes.message_types[message_type]
        if message_type == mtypes.M_PRESENTATION:
            topic = topic + '/' + mtypes.presentation_sub_types[sub_type]
        elif message_type == mtypes.M_SET:
            topic = topic + '/' + mtypes.set_sub_types[sub_type]
        qos=2
        retain=False
        mqttc.publish(topic, payload, qos, retain)
    except ValueError:
        pass

go=True

ser=None
sio=None
sConnected=False
isDebug=False
host='localhost'
port=1883
serialport=''
baud=115200

try:
    opts, args = getopt.getopt(sys.argv[1:], "b:dh:p:s:", ["baud=", "debug", "host=", "port=", "serial="])
except getopt.GetoptError as err:
    print str(err)
    sys.exit(2)

for o, a in opts:
    if o == "-b":
        baud = int(a)
    elif o == "-d":
        isDebug = True
    elif o == "-h":
        host = a
    elif o == "-p":
        port = int(a)
    elif o == "-s":
        serialport = a
    else:
        assert False, "unhandled option"

signal.signal(signal.SIGTERM, sighndlr)

mqttc = mqtt.Client()
mqttc.on_message = on_message
#mqttc.on_publish = on_publish

q = queue.Queue()
try:
    #ser = serial.Serial('/dev/ttyACM0', 115200, timeout=1)
    ser = serial.Serial(serialport, baud, timeout=1)
    sio = io.TextIOWrapper(io.BufferedRWPair(ser, ser))
    info('Connected to Serial')
    sConnected=True
    mqttc.connect(host, port)
    mqttc.subscribe('pmmg/in/#', qos=2)
    while go and sConnected:
        try:
            mqttc.loop_misc()
            line = sio.readline().rstrip()
            if line != '':
                g2m(line)
            mqttc.loop_write()
            while True:
                mqttc.loop_read()
                try:
                   m = q.get(False)
                   m2g(m)
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
        print 'Closed serial port'
        mqttc.disconnect()
        print 'disconnected from mqtt broker'

print 'bye'
