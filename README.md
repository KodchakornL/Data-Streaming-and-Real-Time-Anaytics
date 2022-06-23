# Data-Streaming-and-Real-Time-Anaytics

**Project use Kafka, spark => pyspark**  

## 1. Wordcount Harry every 5 seconds.  
- Show result "Harry" every 5 second => msg = c.poll(5)  
- Show result equal 0 if don't have "Harry" every 5 second  

**Data Achitecture**  
<img src="https://github.com/KodchakornL/Data-Streaming-and-Real-Time-Anaytics/blob/main/slide_ppt/picture_No.1.png" width="450" height="300" />  


**Consumer : Using Kafka**  
  
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
  
  
**Consumer : Using spark => pyspark**
  
          if __name__=="__main__":
              sc = SparkContext(appName="Kafka Spark Demo")
              sc.setLogLevel("WARN")

              ssc = StreamingContext(sc,5)

              msg = KafkaUtils.createDirectStream(ssc, topics=["streams-plaintext-input"],kafkaParams={"metadata.broker.list":"localhost:9092"})

              def rdd_print(rdd):
                  a = rdd.collect()
                  print(f'Harry : {a[0]}')

              words = msg.map(lambda x: x[1]).flatMap(lambda x: x.lower().split(" ")).filter(lambda x:'harry' in x )

              words.count().foreachRDD(rdd_print)


              # wordcount = words(lambda x: ("Harry",1)).reduceByKey(lambda a,b: a+b)
              # wordcount.pprint()

              ssc.start()
              ssc.awaitTermination()
            
            
## 2. Use TF-IDF to find a significant word.
