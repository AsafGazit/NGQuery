# NGQuery 
## Search-term query over N-gram PySpark dataframe (of multiple text files)

NGQuery search for best matches, defined by the search term, over N-gram transfromed documents dataframe.
NGQuery takes N value lists, corresponding to each N-gram part in the N-gram transfromed text dataframe.
NGQuery return the "best match" (highest score or parts in lists provided) for each of the documents in the dataframe.

### "Boosting" weak search rules
Each of the value lists alone, applied on the correstponding NG-part, cannot be a strong indicator for the search-term in any N-gram.
Applying each of the rules by themselves by N simple queries and aggregating the results allowing to search for the highest scoring N-gram in each text document.
This differs from applying a single query with multiple AND clauses as it does not enforce a strong logical AND rule. To replace that, a flexable aggregation of the simplified results allow a more general "desired" term, thus theoretically allowing for typos or OCR errors.

![#NGQuery diagram](https://github.com/AsafGazit/NGQuery/blob/master/img/NGQuery.PNG)

NGQuery operates over text files (documents) transformed to N-grams in a PySpark dataframe. The transformation, of documents to N-grams features, is detailed here.
It compares each column in the N-gram dataframe to a wide array of desired values for that columns. This simple query is easy to configure by defining a list of desired values for a certain N-gram part.

NGQuery allows examining the results of your query over the documents in the PySpark dataframe using embedded plotting functions:
- Examine the values and responses for each ng-part:
![#plot_SearchTermValues function](https://github.com/AsafGazit/NGQuery/blob/master/img/plot_SearchTermValues.jpg)
The plot shows a query looking for a term including a date. The query columns 'ng2' and 'ng3' contain values mostly describing the date name while 'ng4','ng5' and 'ng6' values mostly describe date-assosiated values.
The columns capturing the term name ('ng2', 'ng3') in the plot shows that those terms narrows the search term and infact makes it more term-specific. 
The date columns ('ng4','ng5','ng6') mostly tried to have the relevant values in corresponding values.

- Examine the N-gram distribution over the NGQuery and suggestion scores:
![#plot_SearchResultsValues function](https://github.com/AsafGazit/NGQuery/blob/master/img/plot_SearchResultsValues.jpg)
The plot shows that the query managed to locate a few "perfect sixes" phrases which match the search values perfectly. 
The plot also shows that a few 3,4,5's were picked as suggested values. This suggest either that the search term values needs refining or another NGQuery, using a diffrent assosiative pattern, should be designed to capture those instances.

## Implemented class - NGQuery:
### Notes: 
- NGQuery currently deployed over N-grams of length 6.
- Prerequesits: PySpark, pandas, numpy, matplotlib.

### Init:
NGQuery(PySparkNgramsDataframe,[values for ng part 1],[values2],[values3],[values4],[values5],[values6])

Values are attributs of class, respective to its corresponding N-gram part (DF column):
self.ng1values, self.ng2values, self.ng3values, self.ng4values, self.ng5values, self.ng6values.
ngNvalues are in python list type and are case sensitive.

### Query execution:
-exec_queries() : performs queries
Responses of queries are stored in class attributes, respectivly:
self.ng1res,self.ng2res,self.ng3res,self.ng4res,self.ng5res,self.ng6res
ngNres are pySpark dataframes.

-rate_values() : ranks records according to the responses.
Rank all N-grams according to the responses. 
Enables:
self.allcounts : pySpark dataframe, union of all the responses (ID), counted.
self.top_result_value : pySpark dataframe, top N-gram per document.

### Saving and plotting results :
-save_results_CSV (filename) : saves [self.top_result_value] to a local filename.CSV.
-plot_SearchTermValues(savename=False) : plots the search term values amounts, per ng-part, and the responses for each term values, per file (mean anount and standard deviation shown). 
Save plots to savename (must include .format) to save.
-plot_SearchResultsValues(savename=False) : plots the distribution of N-gram scores, per file (mean anount and standard deviation shown) and the suggested term scores.
Save plots to savename (must include .format) to save.


TBD

Example:

Transforming documents to PySpark N-grams form dataframe:

Applying NGQuery:

