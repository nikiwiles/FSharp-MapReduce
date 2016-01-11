namespace FSharpMapReduce

module Worker =

    open System
    open System.Collections.Generic
    open Akka.Actor
    open Akka.Routing
    open Akka.FSharp
    open Nessos.FsPickler.Json
    open System.Linq
    open FSharpMapReduce.Common

    // Our worker actor; performs the actual Map Reduce work.
    type Worker () = 

        inherit Actor()
            
            // Placeholder for our Map function
            member val mapFn : Option<obj -> KeyValuePair<obj, obj> list> = None with get, set
            
            // Placeholder for our Reduce Function
            member val reduceFn : Option<KeyValuePair<obj, obj> * Dictionary<obj, obj> -> Dictionary<obj, obj>> =  None with get, set

            // Placeholder for the orchestrator (the boss of our workers)
            member val orchestrator : Option<IActorRef> = None with get, set 
            
            // Placeholder for a router that will be used to shuffle mapped data to the correct Worker prior to the Reduce step
            member val shuffleRouter : Option<IActorRef> = None with get, set 
            
            // Holds the data that each Worker has reduced; the Orchestrator will request this data upon completion of the entire Map Reduce job
            member val partialData = Dictionary<obj, obj>() with get, set 

            // Used to hand off data after the Map step and prior to the Reduce step
            member x.shuffle (pair: KeyValuePair<obj, obj>) = 

                // Send our data, wrapped in a ConsistentHashableEnvelope to our shuffleRouter
                // What we actually send is a tuple of the data to send, and the key we'll use to partition the data between Workers, 
                // ... in our case, this key will be the value in the key / value pair.
                let toShuffle = Reduce (pair.Key, pair.Value), pair.Value

                // Wrap and send
                x.shuffleRouter.Value <! ConsistentHashableEnvelope(toShuffle)

            // The Akka message handling logic for our actor
            override x.OnReceive incoming = 
                match incoming with
                | :? WorkerMessage as incoming ->
                        match incoming with 
                        | Router myShuffleRouter -> 
                            
                            // Store a reference to the shuffleRouter
                            x.shuffleRouter <- Some myShuffleRouter

                            // Reply, indicating that the reference has been set correctly
                            x.Sender.Tell(x.shuffleRouter.IsSome, x.Self)

                        | Owner myBoss -> 
                            
                            // Store a reference to the Orchestrator
                            x.orchestrator <- Some myBoss

                            // Reply, indicating that the reference has been set correctly
                            x.Sender.Tell(x.orchestrator.IsSome, x.Self)

                        | SetJob (mapFn, reduceFn) -> 
                            
                            // Unpickle our Map and Reduce functions and store references accordingly
                            x.mapFn    <- Some <| jsonSerializer.UnPickleOfString<obj -> KeyValuePair<obj, obj> list> mapFn
                            x.reduceFn <- Some <| jsonSerializer.UnPickleOfString<KeyValuePair<obj, obj> * Dictionary<obj, obj> -> Dictionary<obj, obj>> reduceFn

                            // Reply, indicating that the references have been set correctly
                            x.Sender.Tell (x.mapFn.IsSome && x.reduceFn.IsSome, x.Self)

                        | Map obj -> 
                            
                            // Apply the map function to our data, which results in a collection of key / value pairs
                            x.mapFn.Value obj |> fun mappedResults -> 

                                // Shuffle each key / value pair via our router, which feed into the Reduce phase 
                                mappedResults |> List.iter(fun result -> x.shuffle result)
                                
                                // Let our orchestrator know that this object has been mapped
                                x.orchestrator.Value <! Mapped

                                // Let our orchestrator know how many key / value pairs it should expect receipts for, after the mapped data has been reduced
                                x.orchestrator.Value <! ( ToReduce <| mappedResults.Count() )

                        | Reduce (key, value) -> 
                            
                            // Data ends up here after shuffling - 
                            // create a key value pair, pass it, along with all data reduced so far, to our Reduce function, and store the results. 
                            new KeyValuePair<obj, obj>(key, value) |> fun pair -> x.partialData <- x.reduceFn.Value(pair, x.partialData)
                            
                            // Let our Orchestrator know that we've reduced an item of data
                            x.orchestrator.Value <! Reduced     

                        | GetResults -> 
                            
                            // Worker replies with its share of the reduced data
                            x.Sender.Tell(x.partialData, x.Self)   

                | _ ->  failwith "Unknown message!"