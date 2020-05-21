from datetime import datetime
import time
import uuid
import paho.mqtt.client as mqtt
from paho.mqtt import publish

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
        client.subscribe("careCenter/app")
        client.subscribe("collectData")

    else:
        print("Bad connection returned code = ", rc)
        print("ERROR: Could not connect to MQTT")


def on_local_message(client, userdata, msg):
    global currentActivity
    arrivedTime = utc_to_local()
    global timestamp
    timestamp = time.time()
    # print(msg.payload)
    global appcount, datacount
    # print("topic:{}".format(msg.topic))

    # if 'collecting' or 'sitting' or 'standing'or 'walk'or 'liedown'or 'collecting' in mgs.payload :
    if mqtt.topic_matches_sub("careCenter/app", msg.topic):
        appcount += 1
        print("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")
        print("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")
        print("appcount={}".format(appcount))
        print("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")
        print("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")
        currentActivity = msg.payload
        # print(currentActivity)
        currentActivity = currentActivity.decode('utf-8')
        # print(currentActivity)
        splitInfo = [x for x in list(currentActivity.split(', ')) if x]
        # print(splitInfo)

        newsensormac = 'Y'
        # id값 및 mac 유니크 검증
        # userid, sensormac, activity에 빈값이 있는지 chk.
        if splitInfo[0]:
            if splitInfo[1]:
                if splitInfo[2]:
                    # 기존에 존재하는 값인지 chk.
                    if sensormac:
                        for i, v in enumerate(sensormac):  # sensor mac 기준으로 입력되어있는 userid 및 activity 확인
                            if v in splitInfo[1]:
                                # print("exist data change")
                                # print("index:{}, value: {}".format(i, v))
                                userid[i] = splitInfo[0]
                                activity[i] = splitInfo[2]
                                # print(userid)
                                # print(sensormac)
                                # print(activity)
                                newsensormac = 'N'
                        if "Y" in newsensormac:
                            userid.append(splitInfo[0])
                            sensormac.append(splitInfo[1])
                            activity.append(splitInfo[2])
                            # print("new sensor mac add")
                            # print(userid)
                            # print(sensormac)
                            # print(activity)
                    else:
                        userid.append(splitInfo[0])
                        sensormac.append(splitInfo[1])
                        activity.append(splitInfo[2])
                        # print("first sensor mac add")
                        # print(userid)
                        # print(sensormac)
                        # print(activity)

    elif mqtt.topic_matches_sub("collectData", msg.topic):
        datacount += 1
        print("datacount={}".format(datacount))
        # print(msg.payload)

    else:
        print("Known topic: %S" % (msg.topic))


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
    client.on_log = on_local_log

    # KST=timezone('Asia/Seoul')

    client.connect(MQTT_HOST, MQTT_PORT)
    client.loop_forever()

print("Test is done!")
