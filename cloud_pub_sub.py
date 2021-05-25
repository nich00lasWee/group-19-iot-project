import time, datetime
import pymysql
import paho.mqtt.client as mqtt
import paho.mqtt.publish as publish

edgeIP = "192.168.13.130"
cloudIP = "192.168.13.131"
edgeTopic = "/topic/Kitchen"
cloudTopic = "/topic/CloudToKitchen"

smokeThreshold = 80

dbConn = pymysql.connect("localhost","pi","","smoke_db") or die("Couldn't connect to database")

def on_connect(client, userdata, flags, rc):
    print("Connected: " + str(rc))
    client.subscribe(edgeTopic)
    
def on_message(client, userdata, msg):
    data = str(msg.payload)
    current = datetime.datetime.now().strftime('%H:%M:%S')
    print("received: " + data + " from " + msg.topic)
    
    #Insert data into database
    with dbConn:
        with dbConn.cursor() as cursor:
            sql = "INSERT INTO smokeLog (smokeVal, timestamp) VALUES (%s, %s)"
            cursor.execute(sql, (data, current))
            dbConn.commit()
            cursor.close()

def calculateAvg():
    # computes the average value from last 100 rows
    with dbConn:
        with dbConn.cursor() as cursor:
            sql = """
SELECT AVG(smokeVal) FROM (
    SELECT smokeVal FROM smokeLog ORDER BY id DESC LIMIT 100
)Var1;
"""
            cursor.execute(sql)
            avg = cursor.fetchone()
            cursor.close()
            
            avg = int(avg[0])
            
    return avg

def main():
    try:
        counter = 0
        while True:
            if (counter > 3):
                avg = calculateAvg()
                
                if (avg > smokeThreshold):
                    msg = "KitchenSmokeHigh"
                    publish.single(cloudTopic, msg, hostname=cloudIP) 
                
                counter = 0    
            
            client = mqtt.Client()
            client.on_connect = on_connect
            client.on_message = on_message
            
            client.connect(cloudIP, 1883, 60)
            
            client.loop_start()
            time.sleep(1)
            client.loop_stop()
            counter += 1
            
    except KeyboardInterrupt:
        print("Stopping...")
        
    finally:
        dbConn.close()
        print("Cleaning up...")        

main()