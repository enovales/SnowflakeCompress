open IdGen
open IronSnappy
open K4os.Compression.LZ4
open System
open System.Collections
open System.IO
open System.IO.Compression

/////////////////////////////////////////////////////////////////////////////
// Simulation tuning knobs

// Specifies the number of bits for each part of the ID structure
// (timestamp, generator ID, sequence number)
let private idStructure = new IdStructure(byte 41, byte 10, byte 12)

// Epoch of the ID format.
let private epoch = new DateTimeOffset(2010, 3, 26, 7, 3, 19, TimeSpan.Zero)

// Duration of each timestamp increment.
let private tickDuration = new TimeSpan(0, 0, 0, 0, 1, 0)

// Upper bound for simulation time increments.
let private generatedTickOffset = 
    int64 (TimeSpan(0, 0, 0, 30, 0, 0).TotalMilliseconds)

// The number of ID generators to use. This can be lower than what is
// allowed by the ID format.
let private idGenMax = 3

// Used to control the probability of IDs generated with the same timestamp,
// but different sequence numbers. This represents the chance, for each
// generated ID, of advancing the timestamp to a new value.
let private probabilityOfAdvancingTick = 0.7

// Controls the number of IDs that are generated for each trial.
let private numberOfIdsToGenerate = 3200

// The number of trials to run.
let private trialRounds = 5000

// The filename to which results will be output.
let private resultsFilename = "results.csv"
/////////////////////////////////////////////////////////////////////////////

type Pipeline = 
    {
        Name: string
        PipelineFunction: int64 array -> byte array
    }

type Trial = 
    {
        Pipelines: Pipeline array
        IdGenerator: unit -> int64 array
    }

type TrialResult = 
    {
        Name: string
        Size: int
    }

type TrialResults = 
    {
        Ids: int64 array
        PipelineResults: TrialResult array
    }

type MockTimeSource(maxOffset: int64, epoch: DateTimeOffset, tickDuration: TimeSpan) = 
    class
        let mutable currentTicks = 0L
        member this.NextTick() = 
            currentTicks <- currentTicks + Random.Shared.NextInt64(maxOffset)
        interface ITimeSource with
            member this.Epoch: System.DateTimeOffset = epoch
            member this.GetTicks(): int64 = currentTicks
            member this.TickDuration: System.TimeSpan = tickDuration
    end

let timeSource = new MockTimeSource(generatedTickOffset, epoch, tickDuration)
let idGenOptions = new IdGeneratorOptions(idStructure = idStructure, timeSource = timeSource)

timeSource.NextTick()

let idGens = [|0..(idGenMax - 1)|] |> Array.map (fun n -> new IdGenerator(n, idGenOptions))
let idGenerator(count: int)() = 
    [| 
        for _ in 1..count do
            let nextGen = idGens.[Random.Shared.Next(idGenMax)]
            if Random.Shared.NextDouble() < probabilityOfAdvancingTick then
                timeSource.NextTick()

            let nextIdValue = nextGen.CreateId()
            yield nextIdValue
    |]

let printIdString(idValue: int64) = 
    printfn "%s" (Convert.ToString(idValue, 2).PadLeft(64, '0'))

let transposeIdBits(ids: int64 array) = 
    let workArea = new BitArray(64 * ids.Length)
    let mutable nextBitIndex = 0
    for j in 0..63 do
        let mask = ((int64 1) <<< j)
        for i in 0..(ids.Length - 1) do
            let cid = ids.[i]
            let nextBitValue = (cid &&& mask) <> 0
            if nextBitValue then
                workArea.Set(nextBitIndex, true)
            nextBitIndex <- nextBitIndex + 1

    let resultArray: int64 array = Array.zeroCreate ids.Length
    nextBitIndex <- 0
    for i in 0..(ids.Length - 1) do
        let mutable nextResult: int64 = 0
        for j in 0..63 do
            if workArea.Get(nextBitIndex) then
                nextResult <- nextResult ||| (1 <<< j)
            nextBitIndex <- nextBitIndex + 1
            resultArray[i] <- nextResult
            
    resultArray

let untransposeIdBits(ids: int64 array) = 
    let bits = ids |> Array.map BitConverter.GetBytes |> Array.collect id
    let workArea = new BitArray(bits)
    let mutable nextBitIndex = 0
    let resultArray: int64 array = Array.zeroCreate ids.Length

    for j in 0..63 do
        for i in 0..ids.Length - 1 do
            if workArea.Get(nextBitIndex) then
                resultArray[i] <- resultArray[i] ||| (1 <<< j)
            nextBitIndex <- nextBitIndex + 1
    resultArray
    
let compressGzip (data: int64[]) =
    use memoryStream = new MemoryStream()
    use gzipStream = new GZipStream(memoryStream, CompressionMode.Compress)
    use writer = new BinaryWriter(gzipStream)

    for value in data do
        writer.Write(value)

    gzipStream.Flush()
    memoryStream.ToArray()

let compressBrotli (data: int64[]) =
    use memoryStream = new MemoryStream()
    use brotliStream = new BrotliStream(memoryStream, CompressionMode.Compress)
    use writer = new BinaryWriter(brotliStream)

    for value in data do
        writer.Write(value)

    brotliStream.Flush()
    memoryStream.ToArray()

let compressZlib (data: int64[]) =
    use memoryStream = new MemoryStream()
    use zlibStream = new ZLibStream(memoryStream, CompressionMode.Compress)
    use writer = new BinaryWriter(zlibStream)

    for value in data do
        writer.Write(value)

    zlibStream.Flush()
    memoryStream.ToArray()

let compressSnappy (data: int64[]) =
    use memoryStream = new MemoryStream()
    use snappyStream = Snappy.OpenWriter(memoryStream)
    use writer = new BinaryWriter(snappyStream)

    for value in data do
        writer.Write(value)

    snappyStream.Flush()
    memoryStream.ToArray()

let compressLZ4 (data: int64[]) =
    let bytes = data |> Array.map BitConverter.GetBytes |> Array.collect id
    let output: byte array = Array.zeroCreate(LZ4Codec.MaximumOutputSize(bytes.Length))
    let encodedLength = LZ4Codec.Encode(bytes, 0, bytes.Length, output, 0, output.Length, LZ4Level.L12_MAX)

    let result: byte array = Array.zeroCreate encodedLength
    Array.Copy(output, result, encodedLength)
    result

let private groupByGeneratorId(k: int64) = 
    let id = idGens[0].FromId(k)
    id.GeneratorId

let private groupByGeneratorIdAndSequenceNumber(k: int64) = 
    let id = idGens[0].FromId(k)
    (id.GeneratorId, id.SequenceNumber)

let runTrial(trial: Trial): TrialResults = 
    let runPipeline(ids: int64 array)(pipeline: int64 array -> byte array) = 
        pipeline(ids)

    let ids = trial.IdGenerator()
    {
        TrialResults.PipelineResults = 
            let computations = 
                trial.Pipelines 
                |> Array.map (fun pipeline -> async { return { TrialResult.Name = pipeline.Name; Size = runPipeline(ids)(pipeline.PipelineFunction) |> Array.length } })

            Async.Parallel(computations) 
            |> Async.RunSynchronously

        Ids = ids
    }

let trial = 
    {
        // The set of experiments to run in each trial. Each experiment will be run with the same set of generated IDs.
        Trial.Pipelines = 
            [|
                { Pipeline.Name = "Original"; PipelineFunction = (fun (ids: int64 array) -> Array.zeroCreate(ids.Length * 8)) }
                { Pipeline.Name = "Gzip"; PipelineFunction = Array.sort >> compressGzip }
                { Pipeline.Name = "Gzip transposed"; PipelineFunction = Array.sort >> transposeIdBits >> compressGzip }
                { Pipeline.Name = "Brotli"; PipelineFunction = Array.sort >> compressBrotli }
                { Pipeline.Name = "Brotli transposed"; PipelineFunction = Array.sort >> transposeIdBits >> compressBrotli }
                { Pipeline.Name = "Zlib"; PipelineFunction = Array.sort >> compressZlib }
                { Pipeline.Name = "Zlib transposed"; PipelineFunction = Array.sort >> transposeIdBits >> compressZlib }
                { Pipeline.Name = "Snappy"; PipelineFunction = Array.sort >> compressSnappy }
                { Pipeline.Name = "Snappy transposed"; PipelineFunction = Array.sort >> transposeIdBits >> compressSnappy }
                { Pipeline.Name = "LZ4"; PipelineFunction = Array.sort >> compressLZ4 }
                { Pipeline.Name = "LZ4 transposed"; PipelineFunction = Array.sort >> transposeIdBits >> compressLZ4 }
                { Pipeline.Name = "LZ4 transposed & sorted by generator ID" 
                  PipelineFunction = 
                    Array.groupBy groupByGeneratorId 
                    >> Array.map (fun (_, ids) -> ids |> Array.sort)
                    >> Array.collect id
                    >> transposeIdBits 
                    >> compressLZ4 }
                { Pipeline.Name = "LZ4 transposed & sort by gen ID and seq #" 
                  PipelineFunction = 
                    Array.groupBy groupByGeneratorIdAndSequenceNumber 
                    >> Array.map (fun (_, ids) -> ids |> Array.sort)
                    >> Array.collect id
                    >> transposeIdBits 
                    >> compressLZ4 }
                { Pipeline.Name = "LZ4 transposed & descending sort by gen ID and seq #" 
                  PipelineFunction = 
                    Array.groupBy groupByGeneratorIdAndSequenceNumber 
                    >> Array.map (fun (_, ids) -> ids |> Array.sortDescending)
                    >> Array.collect id
                    >> transposeIdBits 
                    >> compressLZ4 }
                { Pipeline.Name = "Brotli transposed & sort by gen ID and seq #" 
                  PipelineFunction = 
                    Array.groupBy groupByGeneratorIdAndSequenceNumber 
                    >> Array.map (fun (_, ids) -> ids |> Array.sort)
                    >> Array.collect id
                    >> transposeIdBits 
                    >> compressBrotli }
            |]
        IdGenerator = idGenerator(numberOfIdsToGenerate)
    }

let writer = new StreamWriter(resultsFilename)
trial.Pipelines
|> Array.iter (fun p -> writer.Write(p.Name + ","))
writer.WriteLine()

for _ in 1..trialRounds do
    let results = runTrial(trial)
    results.PipelineResults
    |> Array.iter (fun result -> fprintf writer "%d," result.Size)
    writer.WriteLine()

writer.Close()
