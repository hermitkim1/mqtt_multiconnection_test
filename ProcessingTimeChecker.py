from datetime import datetime
import time
import uuid
import paho.mqtt.client as mqtt
from paho.mqtt import publish
import threading

import threading

MQTT_HOST = "163.180.117.202"
MQTT_PORT = 1883

TEST_TIME = 3600

appcount = 0
datacount = 0
currentActivity = ''
timestamp = ""
newsensormac = 'Y'
userid = []
sensormac = []
activity = []


def utc_to_local():
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')


# -------------------------------------------------------MQTT--------------------------------------------------------#
def on_local_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connected - Result code: " + str(rc))
        # client.subscribe("careCenter/#")
        # client.subscribe("collectData")
        client.subscribe("#")

    else:
        print("Bad connection returned code = ", rc)
        print("ERROR: Could not connect to MQTT")


def get_epochtime_ms():
    return round(datetime.datetime.utcnow().timestamp() * 1000)


prev_time = None
cur_time = None

lock = threading.Lock()


def on_local_message(client, userdata, msg):
    global currentActivity
    arrivedTime = utc_to_local()
    global timestamp
    timestamp = time.time()
    # print(msg.payload)
    global appcount, datacount
    # print("topic:{}".format(msg.topic))
    global prev_time, cur_time
    global lock
    appcount += 1

    lock.acquire()
    prev_time = cur_time
    cur_time = time.perf_counter()
    if prev_time is not None and cur_time is not None:
        diff = cur_time - prev_time
        print("[%s] [Processing time: %7.3fms] [Topic: %s] [%s]" % (
            datetime.now(), diff * 1000, msg.topic, msg.payload))
        # print("Payload: {}".format(msg.payload))
    else:
        print("Unknown")

    # # if 'collecting' or 'sitting' or 'standing'or 'walk'or 'liedown'or 'collecting' in mgs.payload :
    # if mqtt.topic_matches_sub("careCenter/app", msg.topic):
    #     appcount += 1
    #
    # elif mqtt.topic_matches_sub("collectData", msg.topic):
    #     datacount += 1
    #     print("datacount={}".format(datacount))
    #     # print(msg.payload)
    #
    # else:
    #     print("Known topic: %S" % (msg.topic))
    lock.release()


def on_local_publish(client, userdata, mid):
    print("mid: " + str(mid))


def on_local_subscribe(client, userdata, mid, granted_qos):
    print("Subscribed: " + str(mid) + " " + str(granted_qos))


def on_local_log(client, userdata, level, string):
    print(string)


# The below lines will be used to publish the topics
# publish.single("elevator/starting_floor_number", "3", hostname=MQTT_HOST, port=MQTT_PORT)
# publish.single("elevator/destination_floor_number", "2", hostname=MQTT_HOST, port=MQTT_PORT)
# ------------------------------------------------------------------------------------------------------------------#


if __name__ == '__main__':
    client = mqtt.Client()  # client obj creation
    client.on_connect = on_local_connect  # set call back
    client.on_message = on_local_message  # set call back
    client.on_subscribe = on_local_subscribe
    client.on_publish = on_local_publish
    # client.on_log = on_local_log

    # KST=timezone('Asia/Seoul')

    client.connect(MQTT_HOST, MQTT_PORT)
    client.loop_forever()

print("Test is done!")
