# -*- coding: utf-8 -*-
"""
@author: Asaf Gazit


Example of NGQuery


Looking for a term as an "Issue date" :

Looking for a date suggests a few adjecent fields that are indicative of a date.
For that purpose: 
    isinDayofmonth includes numbers between 1 and 31.
    isinYears indludes years between 1950 and 2050.
    isinMonthsnumeric includes numbers 1 to 12.
    isinMonthsnames includes month names.

To make the date term specific, we can define a few key words that the date pattern 
    follows. In this case - "Issue date" or "Settlement date"

Therefore:
    Date pattern is defined by: 
        field A: values are either isinDayofmonth or isinMonthsnames.
        field B: values are either isinDayofmonth or isinMonthsnames.
        (field 1 and 2 are the same to account for both date dypes)
        field C: values are isinYears.
        
    The date name pattern is defined by:
        field D: ["Issue","issue","Settlement","settlement"]
        field E: ["Date","date"]

    It might be suggested that the date will be clearly highlighted and therefore
        will be either directly referenced or included in a numbered list.
        This suggest:
            field F: values between 1 and 40 (numbered list index) or ["Original", "original","a","A","an","An"]

Putting it all together is a query:
'field F' will be followed by the date name pattern and the the date declared, or:    
    NGQuery(ref to Pyspark DF ,field F,field D,field E,field A,field B,field C)

Or in python form:
"""

from pyspark import sql
from pyspark import SparkContext, SparkConf
import numpy as np
import pandas as pd
from pyspark.sql import SQLContext

from NGQuery import NGQuery

## For local execution:
#import findspark

# reference to PySpark directory and set up spark sc and sql context
#PySparkdir = ""
#findspark.init(PySparkdir)
#conf = SparkConf().setAppName("NGQuery").setMaster("local")
#sc = SparkContext(conf=conf)
#sqlContext = SQLContext(sc)
## load pickled dataframe
#loadedngram = sqlContext.createDataFrame(pd.read_pickle('ngramDF.pkl'))


# to execute the query, lists of suggested search terms for each of the N-gram
#   columns needs to be described.

# shortcut arrays
# 
isinAccountnumber = np.arange(1,40).astype(str).tolist()+["Original", "original","a","A","an","An"]
isinDayofmonth = np.arange(32).astype(str).tolist()
isinYears = np.arange(1950,2050).astype(str).tolist()
isinMonthsnumeric = np.arange(13).astype(str).tolist()
isinMonthsnames = ["January","February","March","April","May","June","July"\
                   ,"August","September","October","November","December"]

## defining the query
NGQuery1= NGQuery(loadedngram,isinAccountnumber,["Issue","issue","Settlement","settlement"],["Date","date"],isinDayofmonth+isinMonthsnames,isinMonthsnames+isinDayofmonth,isinYears)
NGQuery1.exec_queries()
NGQuery1.rate_values()
NGQuery1.save_results_CSV("NGQuery1results")
NGQuery1.top_result_value.show()
NGQuery1.plot_SearchTermValues("plot_SearchTermValues.jpg")
NGQuery1.plot_SearchResultsValues("plot_SearchResultsValues.jpg")

#eof