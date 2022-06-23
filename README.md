# Data-Streaming-and-Real-Time-Anaytics

**Project**  

1. Wordcount Harry every 5 seconds.  
  
          while True:
              msg = c.poll(5)
              if msg is None:

                  print('Received message: Harry  , 0')

                  continue
              if msg.error():
                  print("Consumer error: {}".format(msg.error()))
                  continue
              value = msg.value()
              # print(msg.value().decode("utf-8"))
              # print(msg.key())

              if value is None:
                  value = -1
              else:
                  value = int.from_bytes(msg.value(), byteorder="big")
              kvalue = msg.key().decode("utf-8", "ignore")[:6]
              print('Received message: {0} , {1}'.format(kvalue, value))

          c.close()

  
3. Use TF-IDF to find a significant word.
