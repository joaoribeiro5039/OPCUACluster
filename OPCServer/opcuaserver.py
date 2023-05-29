from opcua import ua, Server
import time
import random
import datetime
import json
from opcua.server.event_generator import EventGenerator

# create server
server = Server()
server.name = "SimpleOPCUA"
server.set_endpoint("opc.tcp://localhost:4840")


# Event handler
def event_handler(event):
    print("Node has been written.")
    #print("Node {} has been written.".format(event.Node))


# create objects and variables from json file
with open("nodes.json", "r") as f:
    jsonnodes = json.load(f)


for node in jsonnodes:
    obj = server.nodes.objects.add_object(node["node_id"], node["name"])
    for var in node["variables"]:
        opc_var = obj.add_variable(var["node_id"], var["name"], var["value"])
        opc_var.set_writable(True)
# start server
server.start()

try:
    while True:
        for node in jsonnodes:
            time.sleep(1)
            for var in node["variables"]:
                var_node = server.get_node(var["node_id"])
                if "DateTime" in var["name"]:
                    var_node.set_value(datetime.datetime.now())


finally:
    server.stop()



