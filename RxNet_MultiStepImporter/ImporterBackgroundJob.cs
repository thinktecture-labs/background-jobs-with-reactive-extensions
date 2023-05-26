using System.Reactive;
using System.Reactive.Linq;
using System.Reactive.Subjects;

namespace RxNet_MultiStepImporter;

public class ImporterBackgroundJob : IDisposable
{
   private readonly int _maxConcurrentDownloads;
   private readonly int _maxConcurrentParsings;

   private readonly Subject<long> _onDemandTrigger;
   private readonly IObservable<Unit> _pipeline;

   private long _onDemandCounter = -1; // for logging purposes
   private IDisposable? _runningPipeline;

   public ImporterBackgroundJob(
      TimeSpan importInterval,
      int maxConcurrentDownloads,
      int maxConcurrentParsings)
   {
      _maxConcurrentDownloads = maxConcurrentDownloads;
      _maxConcurrentParsings = maxConcurrentParsings;

      _onDemandTrigger = new Subject<long>();

      _pipeline = Observable.Interval(importInterval)                            // periodic trigger
                            .StartWith(-1)                                       // triggers immediately when calling "Subscribe"
                            .Merge(_onDemandTrigger                              // triggers when calling "EnqueueImport"
                                      .Throttle(TimeSpan.FromMilliseconds(300))) // trigger once if we get multiple triggers within 300ms
                            .SelectThrottle(i => ImportDataFromFtp(i))
                            .Finally(() => Console.WriteLine("Pipeline terminated"));
   }

   public void Start()
   {
      // starts the background job
      _runningPipeline ??= _pipeline.Subscribe();
   }

   public void Stop()
   {
      // stops the background job
      _runningPipeline?.Dispose();
      _runningPipeline = null;
   }

   public void EnqueueImport()
   {
      // starts the import as soon as possible
      _onDemandTrigger.OnNext(Interlocked.Decrement(ref _onDemandCounter));
   }

   private IObservable<Unit> ImportDataFromFtp(long importId)
   {
      // the "Importer" is immutable and thread-safe

      return Observable.Defer(() => Observable.Return(new Importer(importId)))
                       .Do(_ => Console.WriteLine($"[{importId}] ==> Starting import"))
                       .SelectMany(importer => importer.FetchSupplierFtpInfos()
                                                       .ToObservable()
                                                       .SelectAsync((ftpInfo, token) => importer.DownloadFileAsync(ftpInfo, token))
                                                       .Merge(_maxConcurrentDownloads) // degree of parallelism for downloads: 5
                                                       .SelectAsync((file, token) => importer.ParseAsync(file, token))
                                                       .Merge(_maxConcurrentParsings) // degree of parallelism for parsing: 3
                                                       .ToList()                      // collect all results to pass them to SaveToDatabaseAsync all at once
                                                       .Where(d => d.Count > 0)       // ignore
                                                       .SelectManyAsync((data, token) => importer.SaveToDatabaseAsync(data, token))
                                  )
                       .Do(_ => Console.WriteLine($"[{importId}] <== Import finished"))
                       .Catch<Unit, Exception>(ex =>
                                               {
                                                  Console.WriteLine($"[{importId}] <== Import cancelled due to '{ex.GetType().Name}: {ex.Message}'");
                                                  return Observable.Empty<Unit>();
                                               });
   }

   public void Dispose()
   {
      _onDemandTrigger.Dispose();
      _runningPipeline?.Dispose();
   }
}
