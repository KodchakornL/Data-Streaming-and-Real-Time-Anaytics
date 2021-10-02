from typing import NoReturn
import findspark
import re
findspark.init()

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.mllib.feature import HashingTF, IDF
from nltk.corpus import stopwords
# nltk.download('stopwords')
from nltk.tokenize import word_tokenize

from nltk.corpus import stopwords

from nltk.stem import WordNetLemmatizer

counter = 0
counter = counter+1 

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
