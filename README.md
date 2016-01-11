# FSharp-MapReduce

A simple map reduce framework in F# and Akka.Net.

This is a variation on the classic map Reduce algorithm pioneered by Google.

There are three types of actor - 
 
 1) One or more Workers that perform the actual data processing.
 2) An Orchestrator to assign data to workers and collate the results of our Map Reduce job.
 3) A Loader to fetch from a stream of data upon request, and feed it to the Orchestrator for processing.

Algorithm -

 1) Spawn a Loader and an Orchestrator. 

 2) Initialise the Loader with a user defined function that will be used to fetch data from some data source, 
    and send a message to the Orchestrator containing a handle to the Loader, which is stored locally.
  
 3) The Orchestrator is then instructed to spawn some Workers and initialise three routers -
    3a) Broadcast Router   : Broadcasts messages to all of our Workers.
    3b) Shuffle Router     : Partitions data between our Workers by performing a mathematical hash on each item of data.
    3c) Round Robin Router : Sends messages to our workers like dealing a hand of cards, sequentially, in a circular manner.

 4) The Orchestrator is Initialised with three user defined functions -
    4a) Map function            : applies a transformation to an object to produce a collection of key / value pairs.
    4b) Reduce function         : applies a summary function to a key / value pair.
    4c) Handle Results function : the function that we'll apply to the final, blended, results of our Map Reduce job.

 5) The Orchestrator stores the "Handle Results" function locally and broadcasts the Map and Reduce functions to its Workers 
    via the Broadcast Router, which are stored and acknowledged by each of them. 

 6) Given a batch size (which is the maximum number of records to be extracted from the Loader at once) the Orchestrator 
    is instructed to begin processing. The orchestrator then requests the first batch of data from the Loader.

 7) The Orchestrator gets a response from the Loader -
	7a) If the Loader responds with some data, the Orchestrator distributes that data amongst its workers for the Map step, evenly, 
	    using the Round Robin router, and the Orchestrator updates its internal record of the number of mapped records it is expecting
		to receive acknowledgements for.
	7b) If the Loader has no more data, then the loading and mapping steps are marked as complete. 

8) Workers receive data for mapping; for each record they should - 
   8a) Apply the Map function to the data, which produces a collection of key / value pairs. 
   8b) Pass the key / value pair to the Shuffle Router for partitioning, which feeds data into the Reduce phase.
   8c) Send a pair of acknowledgements to the Orchestrator, indicating that - 
	   i) A record has been successfully mapped.
	   ii) The number of key / value pairs that are to be reduced that the Orchestrator should, in turn, expect acknowledgements for.

9) The Orchestrator receives an acknowledgement for an item of mapped data and increments the total tally of acknowledgements received; 
   if the number of acknowledgements is the same as the number of records its expecting to map, then the Orchestrator requests
   another batch of data from the Loader.

10) Workers receive key / value pairs for reducing; for each pair - 
    10a) Take the incoming data and the dictionary of finished results that the Worker has processed thus far, apply the Reduce 
	     function to both of them, and store.
	10b) Send an acknowledgement to the Orchestrator indicating that a key / value pair has been successfully reduced.

11) The Orchestrator receives a message indicating that a key value pair has been reduced, and increments the tally of 
    acknowledgements received. If the number of acknowledgements received is the same as the number of key / value pairs 
	its expecting to reduce, and if mapping and loading has been marked as complete, then request all of the processed data 
	held at of all of the Workers.

12) When all of the Workers have responded to the Orchestrator with dictionaries containing successfully reduced data, 
    the Orchestrator merges all of the results together and applies the "Handle Results" function to the merged dictionary.