namespace FSharpMapReduce

module Orchestrator =

    open System
    open System.Collections.Generic
    open Akka.Actor
    open Akka.Routing
    open Akka.FSharp
    open Nessos.FsPickler.Json
    open System.Linq
    open FSharpMapReduce.Common
    open FSharpMapReduce.Worker

    // This actor (we only need one of these) accepts data, distributes it accross our Workers, sends instructions to Workers and receives results. 
    type Orchestrator () = 

        inherit Actor()
            
            // Placeholder for our Worker actors
            member val workers: Option<IActorRef list> = None with get, set

            // Placeholder for the actor that'll load our data
            member val dataLoader : Option<IActorRef> = None with get, set

            // Router that'll the orchestrator will use to divide work between our Workers
            member val roundRobinRouter : Option<IActorRef> = None with get, set

            // Router used to broadcast messages to all of our Workers
            member val broadcastRouter : Option<IActorRef> = None with get, set

            // Router used to partition data that has been mapped, prior to the Reduce phase
            member val shuffleRouter : Option<IActorRef> = None with get, set

            // Keeps track of the number of records we're expecting to map in total
            member val expectingToMap = 0 with get, set

            // The total number of receipts we've received from workers; each one acknowledges a mapped record
            member val acknowledgedAsMapped = 0 with get, set

            // Keeps a tally of the number of key value pairs we need to reduce, each mapped records can produce zero or more key / value pairs
            member val expectingToReduce = 0 with get, set

            // The total number of receipts we've recieved from workers; each one acknowledges a successfully reduced key / value pair
            member val acknowledgedAsReduced = 0 with get, set

            // The number of records to request from the loader actor in each batch we process
            member val chunkSize = 0 with get, set

            // Flags to indicate job progress
            member val finishedLoading = false with get, set
            member val finishedMapping = false with get, set

            // Placeholder for a function that the completed results of the map reduce job will be applied to when finished
            member val handleResultsFn : Option<Dictionary<obj, obj> -> unit> = None with get, set

            // Helper function to pass a message to a collection of actors, returns true if all actors respond with true, return false otherwise
            static member askAll (nodes: IActorRef list) (message: obj) = 
                nodes
                    |> Seq.map (fun node -> ( node <? message ) |> Async.RunSynchronously)
                    |> Seq.map (fun response -> response.ToString() |> Convert.ToBoolean) 
                    |> Seq.reduce (&&)

            // Helper function to request a batch of data from our Loader actor and distribute the data between our workers
            member x.distributeWork = 
                
                // Done?
                if not x.finishedLoading then 
                    
                    // Data is mapped in batches, so we need to reset this flag each time we grab more data
                    x.finishedMapping <- false

                    // Request a chunk of data from our Loader actor
                    let toSend = (x.dataLoader.Value <? Gimme x.chunkSize) |> Async.RunSynchronously 

                    // Distribute it to our workers for mapping
                    toSend |> Seq.iter (fun record -> x.roundRobinRouter.Value <! ( Map <| record ) )

                    // How many records did we get from the Loader, remember, it could be less than the number we requested!
                    toSend.Count() |> fun toDo -> 

                        // We'll need to see a receipt for each one ...
                        x.expectingToMap <- x.expectingToMap + toDo

                        // Have we finished loading, e.g. has our Loader run out of records?
                        if toDo = 0 then
                            x.finishedLoading <- true 
                            x.finishedMapping <- true // We've also finished mapping 
            
            // The Akka message handling logic for our orchestrator
            override x.OnReceive incoming = 
                match incoming with
                | :? OrchestratorMessage as incoming ->
                        match incoming with 
                        | SetLoader dataLoadActor -> 

                            // Save a reference to our data loader
                            x.dataLoader <- Some dataLoadActor

                            // Reply 
                            x.Sender.Tell(x.dataLoader.IsSome, x.Self)

                        | Spawn numNodes -> 

                            // Fn builds a Worker, with a given ID
                            let nodeFactory id = actorSystem.ActorOf<Worker>(name = id)

                            // Create a collection of Workers and assign names corresponding to their ids, store
                            x.workers <- Some <| ( [1 .. numNodes] |> List.map Convert.ToString |> List.map nodeFactory )

                            // Akka routers to enable us to round robin, broadcast and shuffle messages to our workers
                            x.roundRobinRouter <- Some <| actorSystem.ActorOf(Props.Empty.WithRouter(new RoundRobinGroup(x.workers.Value)), "roundrobin-group")
                            x.broadcastRouter  <- Some <| actorSystem.ActorOf(Props.Empty.WithRouter(new BroadcastGroup(x.workers.Value)), "broadcast-group")
                            x.shuffleRouter    <- Some <| actorSystem.ActorOf(Props.Empty.WithRouter(new ConsistentHashingGroup(x.workers.Value)), "shuffle-group")

                            // Share the shuffle router and a reference to our orchestrator with all of our Worker actors
                            let successShuffle = Orchestrator.askAll x.workers.Value <| Router x.shuffleRouter.Value 
                            let successOrch    = Orchestrator.askAll x.workers.Value <| Owner  x.Self
                    
                            // Return success / fail
                            x.Sender.Tell(successShuffle && successOrch, x.Self)
  
                        | LoadJob (mapFn, reduceFn, handleResults) -> 
                            
                            // Unpickle and store the function that'll be applied to all data when the job is finished
                            x.handleResultsFn <- Some <| jsonSerializer.UnPickleOfString<Dictionary<obj, obj> -> unit> handleResults 

                            // Send all workers our Map and Reduce functions
                            let success = ( Orchestrator.askAll x.workers.Value <| SetJob (mapFn, reduceFn) ) |> Convert.ToBoolean

                            // Reply
                            x.Sender.Tell(success && x.handleResultsFn.IsSome, x.Self)

                        | Start myChunkSize -> 
                            
                            // ChunkSize is the number of records we'll process in each batch - store 
                            x.chunkSize <- myChunkSize

                            // Fetch some data and divide the records between our workers
                            x.distributeWork
                                        
                        | Mapped -> 
                            
                            // We've received a receipt indicating we've mapped another record, tally it up
                            x.acknowledgedAsMapped <- x.acknowledgedAsMapped + 1

                            // Are we all done with this batch? If so, request more work from the Loader actor
                            if x.expectingToMap = x.acknowledgedAsMapped then 
                                x.finishedMapping <- true
                                x.distributeWork
                
                        | ToReduce count ->
                                
                            // A worker is indicating that it has created some more key pairs that will need to be reduced, tally them up     
                            x.expectingToReduce <-  x.expectingToReduce + count

                        | Reduced -> 
                            
                            // A worker is indicating that it has successfully reduced a key pair, record it
                            x.acknowledgedAsReduced <- x.acknowledgedAsReduced + 1

                            // Are we all done?
                            if x.expectingToReduce = x.acknowledgedAsReduced && x.finishedMapping && x.finishedLoading then 
                                
                                // Helper function to combine two dictionaries
                                let MergeDict (dict1: Dictionary<obj, obj>) (dict2: Dictionary<obj, obj>) = 
                                    dict1.Union(dict2)
                                        .ToDictionary( (fun x -> x.Key) , fun (x: KeyValuePair<obj, obj>) -> x.Value )
                                
                                // Get the results from each worker, combine them all together, and then pass the end result to our handle results function 
                                x.workers.Value
                                |> Seq.map (fun node -> ( node <? GetResults ) |> Async.RunSynchronously )
                                |> Seq.map (fun x -> x :> Dictionary<obj, obj>) 
                                |> Seq.reduce (MergeDict)
                                |> fun mergedResults -> x.handleResultsFn.Value mergedResults
                      
                | _ ->  failwith "Unknown message!"