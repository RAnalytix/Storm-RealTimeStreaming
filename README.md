# Storm-RealTimeStreaming
This utilizes Storm for real time streaming on the classic Word Count problem
This assignment is an extension to the WordCount Topology which we discussed in the module. This Topology kept a track of the count of each word in the input stream as shown in the graphics below. 

 



 

 

 

In this WordCountTopology, we used Fields Grouping to send the stream of sentences from “SplitSentence” Bolt to the “WordCount” Bolt. However, this design of our WordCount Topology has a flaw. Imagine a situation where a particular word has a high frequency in the data stream. In such a case, one particular instance of the “WordCount” bolt would have to process a much higher number of words than the other instances of the “WordCount” bolt. This can lead to a severe load imbalance problem. In this assignment, you are required to fix this problem. 

 

You need to modify the code of the classes of the WordCount Topology shared with you in such a way that all the instances of the “WordCount” Bolt get a uniform number of words to process in all cases. Along with this modification, you also need to make the required modifications in the classes of the WordCount Topology to activate the Reliability API provided by Storm. In other words, make the required changes in the classes of the WordCount Topology to ensure that all the tuple that enter in our WordCountTopology is processed at least once. 

 

Here are the instructions that you need to keep in mind while designing the solution:

The Word Count Topology must have the following components

One instance of SentenceSpout

Three instances of SplitSentenceBolt

Four instances of WordCountBolt

One instance of ReportBolt

Your topology should be such that the entire stream of Tuples coming from the SplitSentenceBolt must get uniformly distributed among all the instances of the WordCount Bolt.

Your Topology must store the count of the words in a MySQL database.

Your Topology should also use Reliability API of Storm to ensure at least once processing of input messages.
