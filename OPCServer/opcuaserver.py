from opcua import ua, Server
import time
import random
import datetime
import json
import redis
import threading
from opcua.server.event_generator import EventGenerator

global redisserver
redisserver = redis.Redis(host='localhost', port=6379, decode_responses=True)

Current_OPCServer = redisserver.get("ServerStatus")
Server_Info = []
if (Current_OPCServer == None or Current_OPCServer =="[]"):
    Server_Info = [
    {"Number": 1, "Port": 4841}
    ]
    redisserver.set("ServerStatus",json.dumps(Server_Info))
else:
    Server_Info = json.loads(Current_OPCServer)
    new_server = {"Number": Server_Info[-1]["Number"]+1, "Port": Server_Info[-1]["Port"]+1}
    Server_Info.append(new_server)
    redisserver.set("ServerStatus",json.dumps(Server_Info))
    

# create server
server = Server()
server.name = "SimpleOPCUA" + str(Server_Info[-1]["Number"])
server.set_endpoint( "opc.tcp://localhost:" + str(Server_Info[-1]["Port"]))


# Event handler
def event_handler(event):
    print("Node has been written.")
    # print("Node {} has been written.".format(event.Node))


# create objects and variables from json file
# with open("nodes.json", "r") as f:


redis_OPCstruct = redisserver.get("Struct")
if (redis_OPCstruct == None):
    OPCstruct = [
        {
            "node_id": "ns=1;s=Server1.Machine1",
            "name": "Machine1",
            "variables": [
                {
                    "node_id": "ns=1;s=Server1.Machine1.Speed",
                    "name": "Speed",
                    "value": "0",
                },
                {
                    "node_id": "ns=1;s=Server1.Machine1.Temperature",
                    "name": "Temperature",
                    "value": "0",
                },
                {
                    "node_id": "ns=1;s=Server1.Machine1.State",
                    "name": "State",
                    "value": "0",
                },
                {
                    "node_id": "ns=1;s=Server1.Machine1.DateTime",
                    "name": "DateTime",
                    "value": "0",
                }
            ]
        },
        {
            "node_id": "ns=1;s=Server1.Machine2",
            "name": "Machine2",
            "variables": [
                {
                    "node_id": "ns=1;s=Server1.Machine2.Speed",
                    "name": "Speed",
                    "value": "0",
                },
                {
                    "node_id": "ns=1;s=Server1.Machine2.Temperature",
                    "name": "Temperature",
                    "value": "0",
                },
                {
                    "node_id": "ns=1;s=Server1.Machine2.State",
                    "name": "State",
                    "value": "0",
                },
                {
                    "node_id": "ns=1;s=Server1.Machine2.DateTime",
                    "name": "DateTime",
                    "value": "0",
                }
            ]
        }
    ]
    redisserver.set("Struct", json.dumps(OPCstruct))
else:
    OPCstruct = json.loads(redis_OPCstruct)

# Define a function for subscribing to all channels


def subscribe_to_all_channels():
    global redisserver
    pubsub = redisserver.pubsub()
    pubsub.psubscribe('*')  # Subscribe to all channels

    for message in pubsub.listen():
        if message['type'] == 'pmessage':
            print(f"Received a message on channel {message['channel']}: {message['data']}")
            for var in node["variables"]:
                var_node = server.get_node(message['channel'])
                # print(var_node)
                var_node.set_value(message['data'])


# Start a separate thread for subscribing
subscribe_thread = threading.Thread(target=subscribe_to_all_channels)
subscribe_thread.daemon = True
subscribe_thread.start()

for node in OPCstruct:
    obj = server.nodes.objects.add_object(node["node_id"], node["name"])
    for var in node["variables"]:

        current_redis_value = redisserver.get(var["node_id"])
        if(current_redis_value != None):
            opc_var = obj.add_variable(var["node_id"], var["name"], current_redis_value)
            opc_var.set_writable(True)
        else:
            opc_var = obj.add_variable(var["node_id"], var["name"], var["value"])
            opc_var.set_writable(True)

# start server
server.start()

try:
    a = 0
    while True:
        # for node in OPCstruct:
        #     for var in node["variables"]:
                time.sleep(1)
finally:
    server.stop()
    Current_OPCServer = redisserver.get("ServerStatus")
    Server_Info = []
    Server_Info = json.loads(Current_OPCServer)
    for listed_server in Server_Info:
        server_name = "SimpleOPCUA" + str(listed_server["Number"])
        if server_name == server.name:
            Server_Info.remove(listed_server)
            break
    redisserver.set("ServerStatus",json.dumps(Server_Info))
