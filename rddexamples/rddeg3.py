from pyspark.sql import *
from pyspark.sql.functions import *
import re
import nltk
from nltk.corpus import stopwords

all=nltk.download('stopwords')
stop_words = set(stopwords.words('english'))


spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
sc=spark.sparkContext
data=r"D:\bigdata\drivers\wordcount.txt"
ardd = sc.textFile(data)

skip=['a','and','the','on','to','is','an','','of','be','can','in','that','or','for','an','by','The']
#sk=ardd.first() #return first line of the data.
res=ardd.map(lambda x:re.sub('[^0-9a-zA-Z ]','',x))\
    .flatMap(lambda x:x.split(" "))\
    .filter(lambda x: x not in skip)\
    .map(lambda x:(x,1)).reduceByKey(lambda x,y:x+y)\
    .sortBy(lambda x:x[1],False)

    #.map(lambda x:x.split(" "))
for x in res.take(19):
    print(x)

#in sql ur using groupBy col .. similarly in rdd if u want to group by use "reduceByKey" or"groupByKey"

#imp note: anything ends with "byKey" (reduceByKey, aggregateByKey, groupByKey, sortByKey) input data must be key, value format.

#collect or count not recommended for a large data use take(40) or takeSample alternatively
#if u use map .. the results must be in list like ['Programming', 'Guides']

