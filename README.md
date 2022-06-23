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
TF = (Number of repetitions of word in a document) / (# of words in a document) and  
IDF =Log[(# Number of documents) / (Number of documents containing the word)]  
  
  
#### **Producer**  
Send 1 page at a time of the book.  
  
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
  
  
**Result**
  
  <img src="https://github.com/KodchakornL/Data-Streaming-and-Real-Time-Anaytics/blob/main/slide_ppt/picture_No.2.png" width="900" height="200" />  
  
  
                def update_Num_page(msg):
                    global Num_sentence
                    # docs = ast.literal_eval(msg)
                    # print(msg)
                    Num_sentence = []

                    Num_word_sentence = []

                    text = msg.replace('miss','').replace('mrs.','').replace('mr.','')
                    split_sentence = text.lower().split(".")
                    num_total_sen = len(text.lower().split(" "))

                    # print(split_sentence)
                    for s in range(len(split_sentence)) :
                        st = stop_word(split_sentence[s])
                        # print(st)
                        Tuple_sentence = (s,st,len(st.split(' ')))
                        if split_sentence[s] is not '':
                            Num_sentence.append(Tuple_sentence)


                def Tfidf(masseage):
                    try:
                        global Num_page
                        # Rdd from stream
                        records = masseage.collect()
                        doc = records[0][1]

                        update_Num_page(doc)

                        # print(f'Total Sentence: {Num_sentence}')

                        line = sc.parallelize(Num_sentence)

                        #Find TF
                        linecount = line.map(lambda x : (x[0] ,x[2]))#.reduceByKey(lambda x,y:x+y)
                        # print(linecount.take(10))

                        map_0 = line.flatMap(lambda x: [((x[0],i.strip()),1) for i in x[1].split()])
                        # print(map_0.take(10))

                        reduce = map_0.reduceByKey(lambda x,y:x+y)
                        # print(reduce.take(10))
                        # test = reduce.collect()

                        #Find TF
                        mapForTf = reduce.map(lambda x : (x[0][0],(x[0][1], x[1])))
                        # print(mapForTf.take(10)) 


                        #join mapForTf with linecount
                        joinmapForTF_linecount = mapForTf.join(linecount)
                        # print(joinmapForTF_linecount.take(10)) 

                        TFcal = joinmapForTF_linecount.map(lambda x: (x[1][0][0],(x[0],x[1][0][1]/x[1][1])))
                        # print(TFcal.take(10)) 


                        map3 = reduce.map(lambda x: (x[0][1],(x[0][0],x[1],1))).map(lambda x:(x[0],x[1][2])).reduceByKey(lambda x,y:x+y)
                        # print(map3.take(10))


                        # map4 = map3.map(lambda x:(x[0],x[1][2])).reduceByKey(lambda x,y:x+y)
                        # print(map4.take(10))
                        # reduce2 = map4.reduceByKey(lambda x,y:x+y)
                        # print(reduce2.take(10))

                        # print(len(Num_sentence))
                        idf = map3.map(lambda x: (x[0],math.log10((1+len(Num_sentence))/(1+x[1]))+1))
                        # print(idf.take(10))

                        tfidf = TFcal.join(idf)
                        # print(tfidf.take(10))

                        tfidf = tfidf.map(lambda x: (x[1][0][0],(x[0],x[1][0][1],x[1][1],x[1][0][1]*x[1][1]))).sortByKey()
                        # print(tfidf.take(10))

                        tfidf = tfidf.map(lambda x: (x[0],x[1][0],x[1][1],x[1][2],x[1][3])).sortBy(lambda a: -a[4])
                        print(tfidf.take(10))

                        # for_produce = tfidf.collect()
                        # ms = "word :" + str(for_produce[0][1])

                        # p = Producer({'bootstrap.servers': 'localhost:9092,localhost:9192,localhost:9292'})
                        # sendMsg = ms.encode().decode('utf-8').strip('\n')
                        # p.produce('streams-plaintext-input', sendMsg , callback=delivery_report)

                        # time.sleep(1)
                        # p.flush()

                        print("--------------------------------------------------------------------------------------")


    
    except Exception as e:
        print(f'error: {e}')
        
Result : 
  
<img src="https://github.com/KodchakornL/Data-Streaming-and-Real-Time-Anaytics/blob/main/slide_ppt/picture_No.3.png" width="400" height="900" />  
<img src="https://github.com/KodchakornL/Data-Streaming-and-Real-Time-Anaytics/blob/main/slide_ppt/picture_No.4.png" width="900" height="200" />  
