from opcua import Client
import time
import random

# OPC UA server endpoint URL
url = "opc.tcp://localhost:4840"  # Replace with your server's URL

# Create a client instance
client = Client(url)

try:
    # Connect to the server
    client.connect()
    # Get the root node
    root = client.get_root_node()
        
    while True:
        root = client.get_root_node()
        root.get_child(["0:Objects", "0:Machine1", "0:Speed"]).set_value(random.randint(0, 10000))
        print("0:Objects", "0:Machine1", "0:Speed")
        root.get_child(["0:Objects", "0:Machine1", "0:Temperature"]).set_value(random.randint(0, 10000))
        print("0:Objects", "0:Machine1", "0:Temperature")
        root.get_child(["0:Objects", "0:Machine2", "0:Speed"]).set_value(random.randint(0, 10000))
        root.get_child(["0:Objects", "0:Machine2", "0:Temperature"]).set_value(random.randint(0, 10000))
        print("0:Objects", "0:Machine2", "0:Temperature")
        time.sleep(0.00001)

finally:
    # Disconnect from the server
    client.disconnect()
