namespace RxNet_MultiStepImporter;

public class Importer
{
   // Positive numbers (i.e. 0,1,2,etc.): triggered by periodic trigger
   // -1: triggered on startup
   // Negative numbers starting with -2 (i.e. -2,-3,etc.): triggered by on-demand trigger
   public long ImportId { get; }

   public Importer(long importId)
   {
      ImportId = importId;
   }

   // Fetches FTP url, path and the credentials
   public List<FtpInfo> FetchSupplierFtpInfos()
   {
      Console.WriteLine($"[{ImportId}] Fetching FTP infos.");

      return Enumerable.Range(1, 7)
                       .Select(id => new FtpInfo(id))
                       .ToList();
   }

   public async Task<FtpFile> DownloadFileAsync(FtpInfo ftpInfo, CancellationToken cancellationToken)
   {
      Console.WriteLine($"[{ImportId}] Downloading file from supplier '{ftpInfo.SupplierId}'");
      await Task.Delay(1500, cancellationToken);
      Console.WriteLine($"[{ImportId}]  Downloaded file from supplier '{ftpInfo.SupplierId}'.");

      return new FtpFile(ftpInfo.SupplierId);
   }

   public async Task<Data> ParseAsync(FtpFile file, CancellationToken cancellationToken)
   {
      Console.WriteLine($"[{ImportId}] Parsing file of supplier '{file.SupplierId}'.");
      await Task.Delay(1000, cancellationToken);
      Console.WriteLine($"[{ImportId}]  Parsed file of supplier '{file.SupplierId}'.");

      return new Data(file.SupplierId);
   }

   public async Task SaveToDatabaseAsync(IList<Data> data, CancellationToken cancellationToken)
   {
      Console.WriteLine($"[{ImportId}] Saving data to database. Suppliers: {String.Join(", ", data.Select(d => d.SupplierId))}.");
      await Task.Delay(200, cancellationToken);
      Console.WriteLine($"[{ImportId}]  Saved data to database. Suppliers: {String.Join(", ", data.Select(d => d.SupplierId))}.");
   }
}
