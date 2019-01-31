# -*- coding: utf-8 -*-
"""
@author: Asaf Gazit

Transfroming text documents to N-Grams (6) PySpark Dataframe.


This example includes setting up local PySpark instance and SQLcontext, 
    however, PySpark should be running in the background.

This example also saves the dataframe locally in Pandas' pickle format.
This step is not mandatory and NGQuery can run directly on the resulted PySpark
    dataframe. However, PySpark lazy execution suggests that this transformation 
    will be applied as many times as a query will be executed without making the
    dataframe values available.
    
"""
from pyspark.sql.types import Row
from pyspark.sql.functions import explode
from pyspark import sql
from pyspark import SparkContext, SparkConf
from pyspark.ml.feature import NGram
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.functions import collect_list
from pyspark.sql.functions import input_file_name
from pyspark.sql import SQLContext

## For local execution:
#import findspark

## reference to PySpark directory and set up spark sc and sql context

#PySparkdir = ""
#findspark.init(PySparkdir)
#conf = SparkConf().setAppName("NGQuery").setMaster("local")
#sc = SparkContext(conf=conf)
#sqlContext = SQLContext(sc)

# function to clean the string tokens
def clean_str(x):
  punc='!"#$%�&\'()*+,-./:;<=>?@[\\]^_`{|}~'
  for ch in punc:
    x = x.replace(ch, '')
  return x.strip()

# load all text files in folder txt

# define Row to include all txt in file
row = Row("txt")
loadedtxtclean = sc.textFile('txt\\*').flatMap(lambda line: line.split(" ")).filter(bool).map(clean_str).map(row).toDF(["txt"])
loadedtxtclean=loadedtxtclean.withColumn("input", input_file_name())
documents=loadedtxtclean.groupBy("input").agg(collect_list("txt").alias("txt"))

# create NGRAM df
ngram = NGram(n=6, inputCol="txt", outputCol="ngrams")
ngramcoladdedDataFrame = ngram.transform(documents)
ngramDF = ngramcoladdedDataFrame.select("input",explode("ngrams").alias("ngrams"))
ngramDF = ngramDF.withColumn("ID", monotonically_increasing_id())

# make all ngfunctions
# functions splits N-gram to columns in DF
ngcolnames = ['ng1', 'ng2', 'ng3', 'ng4', 'ng5', 'ng6']

def ngcol1(s):
    return s.split(" ")[0]
def ngcol2(s):
    return s.split(" ")[1]
def ngcol3(s):
    return s.split(" ")[2]
def ngcol4(s):
    return s.split(" ")[3]
def ngcol5(s):
    return s.split(" ")[4]
def ngcol6(s):
    return s.split(" ")[5]

ngcol1_udf = udf(ngcol1, StringType())
ngcol2_udf = udf(ngcol2, StringType())
ngcol3_udf = udf(ngcol3, StringType())
ngcol4_udf = udf(ngcol4, StringType())
ngcol5_udf = udf(ngcol5, StringType())
ngcol6_udf = udf(ngcol6, StringType())

ngramDF = ngramDF.select("input","ID","ngrams", ngcol1_udf("ngrams").alias(ngcolnames[0]),\
                         ngcol2_udf("ngrams").alias(ngcolnames[1]),\
                         ngcol3_udf("ngrams").alias(ngcolnames[2]),\
                         ngcol4_udf("ngrams").alias(ngcolnames[3]),\
                         ngcol5_udf("ngrams").alias(ngcolnames[4]),\
                         ngcol6_udf("ngrams").alias(ngcolnames[5]))

# save dataframe as pickle
ngramDF.toPandas().to_pickle('ngramDF.pkl')

#load pickled dataframe
#loadedngram = sqlContext.createDataFrame(pd.read_pickle('ngramDF.pkl'))

#eof

