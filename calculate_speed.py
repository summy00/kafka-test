from kafka import KafkaProducer, KafkaConsumer
import threading
from threading import Timer
import pickle

kafka_server = "localhost:9092"
messagesize_kb = 100
timeout=30

producer = KafkaProducer(bootstrap_servers=kafka_server)
consumer = KafkaConsumer("mytopic2", bootstrap_servers="localhost:9092", auto_offset_reset='latest', enable_auto_commit=False, group_id=None)
consumer.subscription()

# Function that creates a dummy message of messagesize_kb. We run it first.
def create_message(size_kb):
    char1 = "a".encode("utf8") # This is a 1 byte string
    message="a".encode("utf8")
    for i in range (1,1024*size_kb):
        message=message+char1
    return str(message)


# This is used by the producer to send the data to Kafka
def send(data):
    producer.send(topic="mytopic2", value=data)

# Send messages indefinately.
# It should stop when producer_Sem semaphore is released but this does not work.
# However, it does not affect the throughput measurement.
def produce(message):
    while not producer_sem.acquire(False):
        send(message)


# Consume all messages in the topic and count how many messages were consumed.
# It should stop when consumer_Sem semaphore is released but this does not work.
# However, it does not affect the throughput measurement.
def consume():
    global counter
    counter=0
    while not consumer_sem.acquire(False):
        for msg in consumer:
            data = msg.value
            # Get the message
            counter = counter + 1



# Runs after 30 seconds and calculates messages per second
def finalize():
    total=counter
    print(str(total)+" in 30 seconds.")
    print(str(total/30)+" messages per second.")
    producer_sem.release()
    consumer_sem.release()
    exit(0)


consumer_sem=threading.Semaphore()
producer_sem=threading.Semaphore()
# Create the message. We send the same message over and over again.
message=create_message(messagesize_kb)
my_producer = threading.Thread(target=produce, args=(message,))
my_consumer = threading.Thread(target=consume)
threads = []
threads.append(my_producer)
threads.append(my_consumer)

consumer_sem.acquire()
producer_sem.acquire()
my_consumer.start()
my_producer.start()

# Wait 30 seconds, then run finalize procedure.
t=Timer(30, finalize)
t.start()