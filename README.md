# Data-Streaming-and-Real-Time-Anaytics

**Project use noSQL : Kafka, spark => pyspark**  

## 1. Wordcount Harry every 5 seconds.  
- Show result "Harry" every 5 second => msg = c.poll(5)  
- Show result equal 0 if don't have "Harry" every 5 second  

**Data Achitecture**  
<img src="https://github.com/KodchakornL/Data-Streaming-and-Real-Time-Anaytics/blob/main/slide_ppt/picture_No.1.png" width="750" height="275" />  


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
  
**Producer**  

            for data in Lines:

                p.poll(0)
                time.sleep(0)
                if "Page |" not in data:
                    Temp = Temp+stop_word(data)
                    Temp =Temp.replace('\n',' ')

                else:
                    sendMsg = Temp.encode().decode('utf-8').strip('\n')
                    p.produce('testtopic', sendMsg , callback=delivery_report)
                    print(f'Page {counter_page}')
                    counter_page+=1
                    Temp = ""
                    i=i+1
                    time.sleep(1) 
                if i == 5:
                    break   

            p.flush()
  
  
            def delivery_report(err, msg):
                """ Called once for each message produced to indicate delivery result.
                    Triggered by poll() or flush(). """
                if err is not None:
                    print('Message delivery failed: {}'.format(err))
                else:
                    print('Message delivered to {}'.format(msg.value().decode('utf-8')))

            i =0      
            counter_page = 1
            file1 = open('book.txt', 'r', encoding="utf8") 
            Lines = file1.readlines()
            Temp = ""

            def Punc(test_str):
                punc = '''!()-[]{};:'"\,<>/?@#$%^&*_~’“”'''
                for ele in test_str:
                    if ele in punc:
                        test_str = test_str.replace(ele, "  ")

                return test_str.lower()

            def stop_word(sentence):
                filtered_sent=[]
                # sentence = "Let's see how it's working."
                tokenized_sent = Punc(sentence.lower()).split(' ')
                # print(tokenized_sent)

                for w in tokenized_sent:
                    if w.strip('\n') not in stop_words:
                        filtered_sent.append(w)
                # print("Tokenized Sentence:",tokenized_sent)
                # print("Filterd Sentence:",filtered_sent)
                return ' '.join(filtered_sent)
                
  **Result**
  
  <img src="https://github.com/KodchakornL/Data-Streaming-and-Real-Time-Anaytics/blob/main/slide_ppt/picture_No.2.png" width="900" height="200" />  
