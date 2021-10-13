from confluent_kafka import Consumer

c = Consumer({
    'bootstrap.servers': 'localhost:9092,localhost:9192,localhost:9292',
    'group.id': 'mygroup',
    'auto.offset.reset': 'earliest'
})

c.subscribe(['streams-wordcount-output'])

import matplotlib.pyplot as plt

while True:
    msg = c.poll(1.0)

    if msg is None:
        continue
    if msg.error():
        print("Consumer error: {}".format(msg.error()))
        continue

    kvalue = msg.key().decode("utf-8", "ignore")
    print(kvalue)
    
    
    value = msg.value()
    int_val = int.from_bytes(value, "big")
    print(int_val)
    
    plt.bar(kvalue,int_val, color='r')
    plt.pause(0.1)

plt.show(block=True)
c.close()

