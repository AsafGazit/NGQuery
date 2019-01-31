# -*- coding: utf-8 -*-
"""
@author: Asaf Gazit
NGQuery : Search-term query over N-gram PySpark dataframe (of multiple text files)

NGQuery search for best matches, defined by the search term, over N-gram 
    transformed documents dataframe. NGQuery takes N value lists, corresponding to 
    each N-gram part in the N-gram transformed text dataframe. NGQuery return the 
    "best match" (highest score or parts in lists provided) for each of the 
    documents in the dataframe.

"Boosting" weak search rules
Each of the value lists alone, applied on the corresponding NG-part, cannot be 
    a strong indicator for the search-term in any N-gram. Applying each of the 
    rules by themselves by N simple queries and aggregating the results allowing 
    to search for the highest scoring N-gram in each text document. This differs
    from applying a single query with multiple AND clauses as it does not enforce 
    a strong logical AND rule. To replace that, a flexible aggregation of the 
    simplified results allow a more general "desired" term, thus theoretically 
    allowing for typos or OCR errors.

NGQuery operates over text files (documents) transformed to N-grams in a PySpark
    dataframe. The transformation, of documents to N-grams features, is detailed 
    in the following link : 
 
NGQuery compares each column in the N-gram dataframe to a wide array of desired
    values for that columns. This simple query is easy to configure by defining a 
    list of desired values for a certain N-gram part.

NGQuery allows examining the results of your query over the documents in the
    PySpark dataframe using embedded plotting functions.
    
Class attributes: parameters and functions

Implemented class - NGQuery:
    
Notes:
NGQuery currently deployed over N-grams of length 6.

Prerequesits: PySpark, pandas, numpy, matplotlib.

Init:
#####
NGQuery(PySparkNgramsDataframe,[values for ng part 1],[values2],[values3],[values4],[values5],[values6])
Values are attributes of class, respective to its corresponding N-gram part (DF column):
    self.ng1values, self.ng2values, self.ng3values, self.ng4values,
    self.ng5values, self.ng6values. 
ngNvalues are in python list type and are case sensitive.

Query execution:
################
exec_queries() : performs queries Responses of queries are stored in class attributes,
respectively: self.ng1res,self.ng2res,self.ng3res,self.ng4res,self.ng5res,self.ng6res.

ngNres are pySpark dataframes.

rate_values() : ranks records according to the responses. Rank all N-grams according
to the responses. Enables: 
    self.allcounts : pySpark dataframe, union of all the responses (ID), counted. 
    self.top_result_value : pySpark dataframe, top N-gram per document.

Saving and plotting results :
save_results_CSV (filename) : saves [self.top_result_value] to a local filename.CSV.
plot_SearchTermValues(savename=False) : plots the search term values amounts, 
    per ng-part, and the responses for each term values, per file (mean anount 
    and standard deviation shown). Save plots to savename (must include .format) 
    to save.
plot_SearchResultsValues(savename=False) : plots the distribution of N-gram scores, 
    per file (mean anount and standard deviation shown) and the suggested term scores.
    Save plots to savename (must include .format) to save.
"""



import matplotlib.pyplot as plt
from pyspark import sql
from pyspark import SparkContext, SparkConf
import numpy as np
import pyspark.sql.functions as func
from pyspark.sql.window import Window

## For local execution:
#import findspark

## reference to PySpark directory and set up spark sc and sql context

#PySparkdir = ""
#findspark.init(PySparkdir)
#conf = SparkConf().setAppName("NGQuery").setMaster("local")
#sc = SparkContext(conf=conf)
#sqlContext = sql.SQLContext(sc)

class NGQuery:
    def __init__(self, pysparkNGDF ,ng1values=[], ng2values=[],\
                 ng3values=[],ng4values=[], ng5values=[],\
                 ng6values=[]):
        self.pysparkNGDF = pysparkNGDF
        self.ng1values=ng1values
        self.ng2values=ng2values
        self.ng3values=ng3values
        self.ng4values=ng4values
        self.ng5values=ng5values
        self.ng6values=ng6values
    
    def exec_queries(self):
        self.ng1res=self.pysparkNGDF.filter(self.pysparkNGDF.ng1.isin(self.ng1values)).select("input","ngrams",'ID')
        self.ng2res=self.pysparkNGDF.filter(self.pysparkNGDF.ng2.isin(self.ng2values)).select("input","ngrams",'ID')
        self.ng3res=self.pysparkNGDF.filter(self.pysparkNGDF.ng3.isin(self.ng3values)).select("input","ngrams",'ID')
        self.ng4res=self.pysparkNGDF.filter(self.pysparkNGDF.ng4.isin(self.ng4values)).select("input","ngrams",'ID')
        self.ng5res=self.pysparkNGDF.filter(self.pysparkNGDF.ng5.isin(self.ng5values)).select("input","ngrams",'ID')
        self.ng6res=self.pysparkNGDF.filter(self.pysparkNGDF.ng6.isin(self.ng6values)).select("input","ngrams",'ID')

    def rate_values(self):
        self.allcounts = self.ng1res.union(self.ng2res).union(self.ng3res)\
                    .union(self.ng4res).union(self.ng5res).union(self.ng6res)\
                    .groupBy("input","ngrams",'ID').count().select("input","ngrams",'ID',func.col("count").alias("rulecount"))
        window = Window.\
              partitionBy(self.allcounts.input).\
              orderBy(self.allcounts.rulecount.desc(),self.allcounts.input,self.allcounts.ID,self.allcounts.ngrams)
        tempdf = self.allcounts.withColumn("rank_based_on_rulecount",func.rank().over(window))
        self.top_result_value_unfiltered = tempdf
        top_result_value_df = tempdf.filter(tempdf.rank_based_on_rulecount == 1).select([tempdf.input,tempdf.ngrams,tempdf.ID,tempdf.rulecount])
        self.top_result_value = top_result_value_df
    
    def save_results_CSV(self,filename):
        self.top_result_value.toPandas().to_csv(filename+'.csv')
    
    def plot_SearchTermValues(self, savename=False):
        ngnumberofterms = [len(self.ng1values),len(self.ng2values),len(self.ng3values),\
                           len(self.ng4values),len(self.ng5values),len(self.ng6values)]
        ngnumberoftermsstd = [0,0,0,0,0,0]
        
        numberofresponses = [self.ng1res.groupby("input").count().select("count").agg({"count":"mean"}).first()[0],\
                             self.ng2res.groupby("input").count().select("count").agg({"count":"mean"}).first()[0],\
                             self.ng3res.groupby("input").count().select("count").agg({"count":"mean"}).first()[0],\
                             self.ng4res.groupby("input").count().select("count").agg({"count":"mean"}).first()[0],\
                             self.ng5res.groupby("input").count().select("count").agg({"count":"mean"}).first()[0],\
                             self.ng6res.groupby("input").count().select("count").agg({"count":"mean"}).first()[0]]

        numberofresponsesstd = [self.ng1res.groupby("input").count().select("count").agg({"count":"std"}).first()[0],\
                                self.ng2res.groupby("input").count().select("count").agg({"count":"std"}).first()[0],\
                                self.ng3res.groupby("input").count().select("count").agg({"count":"std"}).first()[0],\
                                self.ng4res.groupby("input").count().select("count").agg({"count":"std"}).first()[0],\
                                self.ng5res.groupby("input").count().select("count").agg({"count":"std"}).first()[0],\
                                self.ng6res.groupby("input").count().select("count").agg({"count":"std"}).first()[0]]

        n_groups = 6
        
        fig, ax = plt.subplots(figsize=(14, 9)) 
        index = np.arange(n_groups)
        bar_width = 0.35
        
        opacity = 0.4
        error_config = {'ecolor': '0.3'}
        
        _ = ax.bar(index, ngnumberofterms, bar_width,
                        alpha=opacity, color='b',
                        yerr=ngnumberoftermsstd, error_kw=error_config,
                        label='Number of search term values')
        
        _ = ax.bar(index + bar_width, numberofresponses, bar_width,
                        alpha=opacity, color='r',
                        yerr=numberofresponsesstd, error_kw=error_config,
                        label='Number of resposes found')
        
        ax.set_xlabel('NGRAM parts')
        ax.set_ylabel('Number of search values and responses per file (mean and standard deviation) per ng in NGRAM')
        ax.set_title('NGQuery search responses per NGRAM part')
        ax.set_xticks(index + bar_width / 2)
        ax.set_xticklabels(['ng1', 'ng2', 'ng3', 'ng4', 'ng5', 'ng6'])
        ax.legend(loc="upper left")
        
        fig.tight_layout()
        if(savename!=False):
            fig.savefig(savename)
        else:
            plt.show()
    def plot_SearchResultsValues(self, savename=False):
        nmresultsmeanarray = np.array(self.allcounts.groupby("input","rulecount").count().groupby("rulecount").agg({"count":"mean"}).sort("rulecount").collect())
        nmresultsstdarray = np.array(self.allcounts.groupby("input","rulecount").count().groupby("rulecount").agg({"count":"std"}).sort("rulecount").collect())
        
        numberofresults = nmresultsmeanarray[:,1]
        ngnumberoftermsstd = nmresultsstdarray[:,1]
        
        result_idx = nmresultsmeanarray[:,0]
        
        ngramsuggestionsrulecounts = np.array(self.top_result_value.groupby("rulecount").count().collect())
        rulecountidx = ngramsuggestionsrulecounts[:,0]
        rulecountvalues = ngramsuggestionsrulecounts[:,1]
        search_result_values=[]
        for idx in result_idx:
            if(idx in rulecountidx):
                mask = np.where(rulecountidx==idx)
                search_result_values.append(rulecountvalues[mask])
            else:
                search_result_values.append(0)
                
        
        suggestedrulecount = search_result_values

        numberofresponsesstd = np.zeros((len(suggestedrulecount)))

        n_groups = len(nmresultsmeanarray[:,0])
        
        fig, ax = plt.subplots(figsize=(14, 9)) 
        index = np.arange(n_groups)
        bar_width = 0.35
        
        opacity = 0.4
        error_config = {'ecolor': '0.3'}
        
        _ = ax.bar(index, numberofresults, bar_width,
                        alpha=opacity, color='b',
                        yerr=ngnumberoftermsstd, error_kw=error_config,
                        label='All NGRAM socres, per file (mean and standard deviation)')
        
        _ = ax.bar(index + bar_width, suggestedrulecount, bar_width,
                        alpha=opacity, color='r',
                        yerr=numberofresponsesstd, error_kw=error_config,
                        label='Suggested NGRAMs scores (sum)')
        
        ax.set_xlabel('NGRAM score')
        ax.set_ylabel('Number of NGRAMS per score and values of suggested NGRAMS')
        ax.set_title('NGQuery search scores and suggested NGRAMS socres')
        ax.set_xticks(index + bar_width / 2)
        ax.set_xticklabels(result_idx)
        ax.legend(loc="upper left")
        fig.tight_layout()
        if(savename!=False):
            fig.savefig(savename)
        else:
            plt.show()
        

# eof
