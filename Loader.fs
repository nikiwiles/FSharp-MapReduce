namespace FSharpMapReduce

module Loader =

    open Akka.FSharp
    open FSharpMapReduce.Common

    // This actor is used to get data from some data source, a batch at a time, upon request
    type Loader () = 

        inherit Actor()
            
            // A marker that indicates how far through the results set we are 
            member val toSkip = 0 with get, set
            
            // Placeholder fn; gets data from a data source, skipping x records, then taking x records 
            member val dataLoadFn : Option<int -> int -> obj list> = None with get, set

            // The Akka message handling logic for our Loader
            override x.OnReceive incoming = 
                match incoming with
                | :? LoaderMessage as incoming ->
                        match incoming with 
                        | Job myLoadFn -> 
                            
                            // Unpickle and store a reference to our loader function
                            x.dataLoadFn <- Some <| jsonSerializer.UnPickleOfString<int -> int -> obj list> myLoadFn

                            // Reply with the results
                            x.Sender.Tell(x.dataLoadFn.IsSome, x.Self)
                             
                        | Gimme take -> 
                            
                            // Fetch x records and reply 
                            x.Sender.Tell(x.dataLoadFn.Value x.toSkip take, x.Self)

                            // Move the marker that represents our current position in the results set
                            x.toSkip <- x.toSkip + take

                | _ ->  failwith "Unknown message!"