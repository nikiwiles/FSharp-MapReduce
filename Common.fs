namespace FSharpMapReduce

module Common =

    open System
    open System.Collections.Generic
    open Akka.Actor
    open Akka.Routing
    open Akka.FSharp
    open Nessos.FsPickler.Json
    open System.Linq

    // We'll use FsPickler to easily ship data and functions between the different components of the system
    let jsonSerializer = FsPickler.CreateJsonSerializer(indent = false)

    // The name of the Akka actor system we're creating
    let ActorSystemName = "MapReduce"

    // Create a new actor system
    let actorSystem = ActorSystem.Create ActorSystemName

    // The types of Akka message that our Orchestrator actor can receive 
    type OrchestratorMessage = 
        | SetLoader        of IActorRef                     // Used to send the Orchestrator a handle for the Loader actor; needed for the Orchestrator to request batches of data.
        | Spawn            of int                           // Instructs the Orchestrator to create a number of Worker actors.          
        | LoadJob          of string * string * string      // Three functions pickled as strings: the Map function, Reduce function, and a function that the results of the entire job are passed to upon completion.          
        | Start            of int                           // Tells the Orchestrator to begin processing, and how many records to request from the Loader actor in each batch. 
        | Mapped                                            // Receipt sent by workers, the Orchestrator should receive one of these for each record to be mapped.
        | ToReduce         of int                           // Each record that gets mapped produces a collection of key / value pairs, and for each object to be mapped, workers let the Orchestrator know how many key / value pairs are to be reduced.
        | Reduced                                           // Receipt sent by workers, the Orchestrator should recieve one of these for each record to be reduced.

    // The types of Akka message that our Worker actors can recieve 
    type WorkerMessage = 
        | SetJob           of string * string               // The Orchestrator will forward serialised map and reduce functions to all the workers using this message type; these will be unpickled and stored.  
        | Router           of IActorRef                     // An Akka router, broadcast by the Orchestrator to all workers. Workers hand-off data to this router after a record has been mapped and it is ready for shuffling. 
        | Owner            of IActorRef                     // The Orchestrator sends all workers a reference to itself, so that workers can send receipts for each record mapped and key / value pair reduced.
        | Map              of obj                           // Incomming data to be passed to the worker's Map function.  
        | Reduce           of obj * obj                     // After the shuffle step, workers receive key / value pairs via this message type that are passed to the Reduce function.
        | GetResults                                        // Using the receipts it receives, the Orchestrator can figure out when the overall job is complete, then request all intermediate results from the workers using this message type.

    // The types of Akka message that our Loader actor(s) can receive. 
    // ... this acts sort of like an interface, e.g. you can have different Loader implementations for different types of data source. 
    type LoaderMessage = 
        | Job of string                                     // A pickled function that's used to request data from the data source.
        | Gimme of int                                      // When a Loader recieves this message, it should respond with n records from the data source. 