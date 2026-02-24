from kafka import KafkaProducer
from time import sleep
file_name=r"C:\tmp\log\apache2\access_log_20231207-075811.log"
producer = KafkaProducer ( bootstrap_servers='localhost:9092' )
with open ( file_name , 'r' ) as file:
    # Read all lines from the file
    for line in file:
        print(line)
        producer.send ( "dec7" , line.encode("ascii") )
#        "dec7","233.102.213.116 - - [07/Dec/2023:08:05:41 +0530] \"DELETE /list HTTP/1.0\" 200 5064 "http://salinas.com/register/" \"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_4; rv:1.9.6.20) Gecko/2020-05-17 02:39:10 Firefox/3.6.14"
        sleep(3)
        #by default kafka within a second it ll send 1 million messages ...sleep ..dont send every second send every 3 seconds

