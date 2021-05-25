import serial
import time
import paho.mqtt.client as mqtt
import paho.mqtt.publish as publish

device = '/dev/ttyACM0'
arduino = serial.Serial(device, 9600, timeout=1)

edgeIP = "192.168.13.130"
cloudIP = "192.168.13.131"
edgeTopic = "/topic/Kitchen"
cloudTopic = "/topic/CloudToKitchen"

def on_connect(client, userdata, flags, rc):
    client.subscribe(cloudTopic)
    
def on_message(client, userdata, msg):
    payload = str(msg.payload)
    print(msg.topic + " " + payload)
    
    # Signal Arduino if smoke threshold is exceeded
    if (payload == "KitchenSmokeHigh"):
        arduino.write(str(1).encode('utf-8') + b'\n')

def main():
    try:
        client = mqtt.Client()
        while True:
            # Read data from Arduino
            data = arduino.readline().decode('utf-8').rstrip()
            
            # Publish smoke data to cloud
            if (data != ""):
                print("Smoke: " + data)
                publish.single(edgeTopic, data, hostname=cloudIP)

            # Read from publisher
            client.on_connect = on_connect
            client.on_message = on_message
            client.connect(cloudIP, 1883, 60)
            
            client.loop_start()
            time.sleep(1)
            client.loop_stop()
        
    except KeyboardInterrupt:
        print("Stopping...")
        
    finally:
        print("Cleaning up...")

main()