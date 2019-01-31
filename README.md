# NGQuery 
## Search-term query over N-gram PySpark dataframe (of multiple text files)

NGQuery search for best matches, defined by the search term, over N-gram transformed documents dataframe.
NGQuery takes N value lists, corresponding to each N-gram part in the N-gram transformed text dataframe.
NGQuery return the "best match" (highest score or parts in lists provided) for each of the documents in the dataframe.

### "Boosting" weak search rules
Each of the value lists alone, applied on the corresponding NG-part, cannot be a strong indicator for the search-term in any N-gram.
Applying each of the rules by themselves by N simple queries and aggregating the results allowing to search for the highest scoring N-gram in each text document.
This differs from applying a single query with multiple AND clauses as it does not enforce a strong logical AND rule. To replace that, a flexible aggregation of the simplified results allow a more general "desired" term, thus theoretically allowing for typos or OCR errors.

![#NGQuery diagram](https://github.com/AsafGazit/NGQuery/blob/master/img/NGQuery.png)

NGQuery operates over text files (documents) transformed to N-grams in a PySpark dataframe. The transformation, of documents to N-grams features, is detailed here.
It compares each column in the N-gram dataframe to a wide array of desired values for that columns. This simple query is easy to configure by defining a list of desired values for a certain N-gram part.

NGQuery allows examining the results of your query over the documents in the PySpark dataframe using embedded plotting functions:
- Examine the values and responses for each ng-part:

![#plot_SearchTermValues function](https://github.com/AsafGazit/NGQuery/blob/master/img/plot_SearchTermValues.jpg)

The plot shows a query looking for a term including a date. The query columns 'ng2' and 'ng3' contain values mostly describing the date name while 'ng4','ng5' and 'ng6' values mostly describe date-associated values.
The columns capturing the term name ('ng2', 'ng3') in the plot shows that those terms narrows the search term and in fact makes it more term-specific. 
The date columns ('ng4','ng5','ng6') mostly tried to have the relevant values in corresponding values.

- Examine the N-gram distribution over the NGQuery and suggestion scores:

![#plot_SearchResultsValues function](https://github.com/AsafGazit/NGQuery/blob/master/img/plot_SearchResultsValues.jpg)

The plot shows that the query managed to locate a few "perfect sixes" phrases which match the search values perfectly. 
The plot also shows that a few 3,4,5's were picked as suggested values. This suggest either that the search term values needs refining or another NGQuery, using a different associative pattern, should be designed to capture those instances.

## Implemented class - NGQuery:
### Notes: 
- NGQuery currently deployed over N-grams of length 6.
- Prerequesits: PySpark, pandas, numpy, matplotlib.
- [Link to source](https://github.com/AsafGazit/NGQuery/blob/master/src/NGQuery.py "Link to source.")

### Init:
NGQuery(PySparkNgramsDataframe,[values for ng part 1],[values2],[values3],[values4],[values5],[values6])

Values are attributes of class, respective to its corresponding N-gram part (DF column):
self.ng1values, self.ng2values, self.ng3values, self.ng4values, self.ng5values, self.ng6values.
ngNvalues are in python list type and are case sensitive.

### Query execution:
- exec_queries() : performs queries
Responses of queries are stored in class attributes, respectively:
self.ng1res,self.ng2res,self.ng3res,self.ng4res,self.ng5res,self.ng6res
ngNres are pySpark dataframes.

- rate_values() : ranks records according to the responses.
Rank all N-grams according to the responses. 
Enables:
self.allcounts : pySpark dataframe, union of all the responses (ID), counted.
self.top_result_value : pySpark dataframe, top N-gram per document.

### Saving and plotting results :
- save_results_CSV (filename) : saves [self.top_result_value] to a local filename.CSV.
- plot_SearchTermValues(savename=False) : plots the search term values amounts, per ng-part, and the responses for each term values, per file (mean anount and standard deviation shown). 
Save plots to savename (must include .format) to save.
- plot_SearchResultsValues(savename=False) : plots the distribution of N-gram scores, per file (mean anount and standard deviation shown) and the suggested term scores.
Save plots to savename (must include .format) to save.

Example:
Transforming documents to PySpark N-grams form dataframe:
[Link to source](https://github.com/AsafGazit/NGQuery/blob/master/src/DOCtoPySparkDF.py "Link to source.")

- This example includes setting up local PySpark instance and SQLcontext, however, PySpark should be running in the background.
- This example also saves the dataframe locally in Pandas' pickle format. This step is not mandatory and NGQuery can run directly on the resulted PySpark dataframe. However, PySpark lazy execution suggests that this transformation will be applied as many times as a query will be executed without making the dataframe values available.

```python
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
```
Applying NGQuery:
[Link to source](https://github.com/AsafGazit/NGQuery/blob/master/src/NGQueryExample.py "Link to source.")

Looking for a term as an "Issue date" :

Looking for a date suggests a few adjecent fields that are indicative of a date.
For that purpose: 
- isinDayofmonth includes numbers between 1 and 31.
- isinYears indludes years between 1950 and 2050.
- isinMonthsnumeric includes numbers 1 to 12.
- isinMonthsnames includes month names.

To make the date term specific, we can define a few key words that the date pattern follows. In this case - "Issue date" or "Settlement date".

Therefore, date pattern is defined by: 
- field A: values are either isinDayofmonth or isinMonthsnames.
- field B: values are either isinDayofmonth or isinMonthsnames.
 (field 1 and 2 are the same to account for both date dypes)
- field C: values are isinYears.
        
The date name pattern is defined by:
- field D: ["Issue","issue","Settlement","settlement"]
- field E: ["Date","date"]

It might be suggested that the date will be clearly highlighted (directly referenced or included in a numbered list). This suggest:
- field F: values between 1 and 40 (numbered list index) or ["Original", "original","a","A","an","An"]

Putting it all together in a query:
'field F' will be followed by the date name pattern and the the date declared, or:    
    NGQuery(ref to Pyspark DF ,field F,field D,field E,field A,field B,field C)

```python
# shortcut arrays
isinAccountnumber = np.arange(1,40).astype(str).tolist()+["Original", "original","a","A","an","An"]
isinDayofmonth = np.arange(32).astype(str).tolist()
isinYears = np.arange(1950,2050).astype(str).tolist()
isinMonthsnumeric = np.arange(13).astype(str).tolist()
isinMonthsnames = ["January","February","March","April","May","June","July"\
                   ,"August","September","October","November","December"]

## defining the query
NGQuery1= NGQuery(loadedngram,isinAccountnumber,["Issue","issue","Settlement","settlement"],["Date","date"],isinDayofmonth+isinMonthsnames,isinMonthsnames+isinDayofmonth,isinYears)

## running the query
NGQuery1.exec_queries()
NGQuery1.rate_values()

## save and visualise
NGQuery1.save_results_CSV("NGQuery1results")
NGQuery1.top_result_value.show()
NGQuery1.plot_SearchTermValues("plot_SearchTermValues.jpg")
NGQuery1.plot_SearchResultsValues("plot_SearchResultsValues.jpg")
```
