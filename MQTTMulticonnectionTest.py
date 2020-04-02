import time
import uuid
import paho.mqtt.client as mqtt
from paho.mqtt import publish

import threading

MQTT_HOST = "163.180.117.202"
MQTT_PORT = 1883

TEST_TIME = 3600


def publish_every_1sec(mqtt_obj, cli_id):

    start_time = time.time()

    while True:
        mqtt_obj.publish("chience/multiconnectiontest/" + cli_id + "/heartbeatmsg", "Can you feel my heart beat?", qos=2)

        running_time = time.time() - start_time
        if running_time > TEST_TIME:
            break
        time.sleep(1)


# -------------------------------------------------------MQTT--------------------------------------------------------#
def on_local_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connected - Result code: " + str(rc))
        # client.subscribe("edge/client/" + client_id + "/data")
        # client.subscribe("edge/client/" + client_id + "/start_caching")

    else:
        print("Bad connection returned code = ", rc)
        print("ERROR: Could not connect to MQTT")


def on_local_message(client, userdata, msg):

    message = msg.payload
    print("Arrived topic: %s" % msg.topic)

    # if msg.topic == "edge/client/" + client_id + "/data":
    #     print("aa")
    # elif msg.topic == "edge/client/" + client_id + "/start_caching":
    #     print("bb")
    #
    # else:
    #     print("Unknown - topic: " + msg.topic + ", message: " + message)


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

    mqtt_clients = []
    threads = []
    client_counter = 0
    clinet_max_number = 200

    print("Create MQTT clients and threads")
    while client_counter < clinet_max_number:
        client_id = str(uuid.uuid1())
        mqtt_client = mqtt.Client(client_id)            # A unique ID will be randomly generated.
        mqtt_clients.append(mqtt_client)                # Add mqtt client object to a list

        mqtt_client.connect(MQTT_HOST, MQTT_PORT, 60)   # Connect to the MQTT Broker
        mqtt_client.loop_start()

        # Creating threads
        thr = threading.Thread(target=publish_every_1sec, args=[mqtt_client, client_id])
        thr.start()
        threads.append(thr)

        client_counter += 1

    print("Join threads")
    client_counter = 0
    while client_counter < clinet_max_number:
        threads[client_counter].join()

    print("Stop and disconnect MQTT clients")
    client_counter = 0
    while client_counter < clinet_max_number:
        mqtt_clients[client_counter].loop_stop()
        mqtt_clients[client_counter].disconnect()

    print("Test is done!")
