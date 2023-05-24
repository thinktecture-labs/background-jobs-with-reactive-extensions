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
      return Observable.Return(Unit.Default)
                       .Do(_ => Console.WriteLine($"[{importId}] ==> Starting import"))
                       .SelectMany(_ => FetchSupplierFtpInfos(importId))
                       .SelectAsync((ftpInfo, token) => DownloadFileAsync(ftpInfo, token))
                       .Merge(_maxConcurrentDownloads) // degree of parallelism for downloads: 5
                       .SelectAsync((file, token) => ParseAsync(file, token))
                       .Merge(_maxConcurrentParsings) // degree of parallelism for parsing: 3
                       .ToList()                      // collect all results to pass them to SaveToDatabaseAsync all at once
                       .Where(d => d.Count > 0)       // ignore
                       .SelectManyAsync((data, token) => SaveToDatabaseAsync(importId, data, token))
                       .Do(_ => Console.WriteLine($"[{importId}] <== Import finished"))
                       .Catch<Unit, Exception>(ex =>
                                               {
                                                  Console.WriteLine($"[{importId}] <== Import cancelled due to '{ex.GetType().Name}: {ex.Message}'");
                                                  return Observable.Empty<Unit>();
                                               });
   }

   private List<FtpInfo> FetchSupplierFtpInfos(long importId)
   {
      Console.WriteLine($"[{importId}] Fetching FTP infos.");

      return Enumerable.Range(1, 7)
                       .Select(id => new FtpInfo(importId, id))
                       .ToList();
   }

   private async Task<FtpFile> DownloadFileAsync(FtpInfo ftpInfo, CancellationToken cancellationToken)
   {
      Console.WriteLine($"[{ftpInfo.ImportId}] Downloading file from supplier '{ftpInfo.SupplierId}'");
      await Task.Delay(1500, cancellationToken);
      Console.WriteLine($"[{ftpInfo.ImportId}]  Downloaded file from supplier '{ftpInfo.SupplierId}'.");

      return new FtpFile(ftpInfo.ImportId, ftpInfo.SupplierId);
   }

   private async Task<Data> ParseAsync(FtpFile file, CancellationToken cancellationToken)
   {
      Console.WriteLine($"[{file.ImportId}] Parsing file of supplier '{file.SupplierId}'.");
      await Task.Delay(1000, cancellationToken);
      Console.WriteLine($"[{file.ImportId}]  Parsed file of supplier '{file.SupplierId}'.");

      return new Data(file.SupplierId);
   }

   private async Task SaveToDatabaseAsync(long importId, IList<Data> data, CancellationToken cancellationToken)
   {
      Console.WriteLine($"[{importId}] Saving data to database. Suppliers: {String.Join(", ", data.Select(d => d.SupplierId))}.");
      await Task.Delay(200, cancellationToken);
      Console.WriteLine($"[{importId}]  Saved data to database. Suppliers: {String.Join(", ", data.Select(d => d.SupplierId))}.");
   }

   public void Dispose()
   {
      _onDemandTrigger.Dispose();
      _runningPipeline?.Dispose();
   }
}
