namespace FSharpMapReduce

module ExampleJob =
    
    // EXAMPLE MAP REDUCE JOB
    // 
    // Load function           : Acts as a stream of dice rolls (1..6); stream terminates after a random number of rolls.
    // Map function            : Transforms a dice roll into a win / lose (6 is a win, otherwise roll is a loser). 
    // Reduce function         : Counts the frequency of wins and losses.
    // Handle Results function : Prints percentage of wins, vs. expectation from a sequence of fair dice rolls. 

    open System
    open System.Collections.Generic
    open System.Linq
    open Akka.Actor
    open Akka.Routing
    open Akka.FSharp
    open Nessos.FsPickler.Json
    open FSharpMapReduce.Common
    open FSharpMapReduce.Loader
    open FSharpMapReduce.Orchestrator
    open FSharpMapReduce.Orchestrator

    // FsPickler: for shipping data and functions
    let jsonSerializer = FsPickler.CreateJsonSerializer(indent = false)

    // The number of map reduce workers that our system will deploy.
    let numberOfWorkers = 8

    // Create an Orchestrator
    let orchestrator = actorSystem.ActorOf<Orchestrator>()

    // Create a Loader
    let loader = actorSystem.ActorOf<Loader>()

    // Random number generator
    let Random = new System.Random()

    // How much data should our Loader generate, before halting? 
    let loaderLimit = Random.Next(1000000, 10000000)

    // Output the number of rolls we'll be performing
    String.Format("Iterations (length of stream): {0} rolls.", loaderLimit) |> Console.WriteLine

    // Our loader function simulates rolling a random number of 6 sided dice, halting after a random number of rolls.
    let myLoadFn skip take = 
        let reduce = Math.Max(skip + take, loaderLimit) - loaderLimit
        let toTake = Math.Max(0, take - reduce)
        [1 .. toTake] |> List.map(fun x -> Random.Next(1, 7) |> box) 

    // MAP FUNCTION: maps a number 1..6 to a WinOrlose, Win if 6, Lose otherwise
    let myMapFn (toMap: obj) = 
        if toMap |> Convert.ToInt32 = 3 then "Win" else "Lose"
        |> fun winLose -> [ new KeyValuePair<obj, obj>("Result" |> box, winLose |> box) ] 

    // REDUCE FUNCTION: Merge the Win / Lose into interim results dictionary, counting the overall freq. of wins and losses.
    let myReduceFn (todo: KeyValuePair<obj, obj>, target: Dictionary<obj, obj>) = 
        match target.TryGetValue(todo.Value) with 
        | (true, value) -> target.[todo.Value] <- box (Convert.ToInt32(value) + 1 ) 
        | _ -> target.Add(todo.Value, box (1))
        target
       
    // HANDLE RESULTS: Convert, and print the results
    let myHandleResultsFn (results: Dictionary<obj, obj>) = 
        let asPercentage (value:decimal) = value.ToString("P8")
        let wins = results.["Win"] |> Convert.ToDecimal
        let losses = results.["Lose"] |> Convert.ToDecimal
        let winRate = wins / (losses + wins)
        String.Format("Percentage of wins: {0}, expected: {1}.", asPercentage winRate, asPercentage (1.0M / 6.0M) ) |> Console.WriteLine

    // Pickle our functions
    let loadFn          = jsonSerializer.PickleToString myLoadFn
    let MapFn           = jsonSerializer.PickleToString myMapFn
    let ReduceFn        = jsonSerializer.PickleToString myReduceFn
    let handleResultsFn = jsonSerializer.PickleToString myHandleResultsFn

    // Send the load function to our Loader actor
    if (loader <? Job(loadFn) ) |> Async.RunSynchronously then 
        // Send a reference to our Loader actor to our Orchestrator
        if (orchestrator <? SetLoader loader) |> Async.RunSynchronously then 
            // Spawn our Workers, routers.
            if ( orchestrator <? Spawn numberOfWorkers ) |> Async.RunSynchronously then 
                // Load our Map Reduce job
                if (orchestrator <? LoadJob (MapFn, ReduceFn, handleResultsFn) ) |> Async.RunSynchronously then
                    // Start loading data, specifying the number of records to pass in each batch
                    orchestrator <! Start 1000

    // Wait key
    Console.ReadKey() |> ignore