# Merging-Event-Datasets
This repository contains a map reduce implementation of a merge between two event datasets.

<p>Let's imagine that we have a dataset containing all the taxis in town, with each record showing whether it is hired, available or off-shift. So, it looks like this<p />

<br>ItemID,datetimeStart,datetimeEnd,state<br />
TaxiA,2015-10-11 19:02:19,2015-10-11 19:15:12,Hired
TaxiA,2015-10-11 19:15:12,2015-10-11 19:18:19,Available
TaxiA,2015-10-11 19:18:19,2015-10-11 19:40:18,Hired
TaxiA,2015-10-11 19:40:18,2015-10-11 20:05:01,Available
TaxiB,2015-10-11 19:11:00,2015-10-11 23:00:45,Off-Shift
TaxiC,2015-10-11 19:11:19,2015-10-11 19:30:27,Available
TaxiE,2015-10-11 19:16:50,2015-10-11 19:46:18,Hired
TaxiF,2015-10-11 19:46:18,2015-10-11 20:10:11,Available

And we have another dataset that contains the taxis' speed. If <30km/h => low, between 30 to 60 km/h => medium, above 60km/h => high. 

datetimeStart,datetimeEnd,itemID,attribute
2015-10-11 19:20:19,2015-10-11 19:21:22,TaxiA,Medium
2015-10-11 19:21:22,2015-10-11 19:25:38,TaxiA,High
2015-10-11 19:25:38,2015-10-11 19:30:01,TaxiA,Medium
2015-10-11 19:30:01,2015-10-11 19:50:01,TaxiA,Low
2015-10-11 20:27:38,2015-10-11 20:32:19,TaxiB,Medium

We want to merge these two datasets in order to relate the taxi speeds to the hired status. So, we want an output like this.

datetimeStart,datetimeEnd,itemID,attribute,state
2015-10-11 19:20:19,2015-10-11 19:21:22,TaxiA,Medium,Hired
2015-10-11 19:21:22,2015-10-11 19:25:38,TaxiA,High,Hired
2015-10-11 19:25:38,2015-10-11 19:30:01,TaxiA,Medium,Hired
2015-10-11 19:30:01,2015-10-11 19:40:18,TaxiA,Low,Hired
2015-10-11 19:40:18,2015-10-11 19:50:01,TaxiA,Low,Available

With this merged dataset, we can begin to analyze whether the taxis drive more slowly when they are available, do taxis drive more in the high or medium or low speeds when they have passengers, etc.

Map reduce is a very natural way to implement the merge logic, as it can be distributed by the itemID (for example, taxis).
Furthermore, we can utilize the efficient distributed sort in map reduce. Here I have implemented a tertiary sort. 
The basic logic is as follows:

1. Group by itemID
2. For each record, generate two records, one for start datetime, one for end datetime. 
3. Sort by datetime within the itemID partitions.
4. For each sorted group of itemID, keep a variable called currentState and currentAttribute.
5. Iterate over the sorted list of events, changing the variables and writing the records to disk one at a time.

All the best!
