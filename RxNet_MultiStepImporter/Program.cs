using RxNet_MultiStepImporter;

var importerJob = new ImporterBackgroundJob(TimeSpan.FromSeconds(60),
                                            maxConcurrentDownloads: 5,
                                            maxConcurrentParsings: 3);
SetupKeyPressListener(importerJob);

importerJob.Start();

await Task.Delay(TimeSpan.FromMinutes(5));

static void SetupKeyPressListener(ImporterBackgroundJob importer)
{
   Task.Run(() =>
            {
               while (true)
               {
                  var key = Console.ReadKey();

                  switch (key.KeyChar)
                  {
                     // enqueue
                     case 'e':
                        importer.EnqueueImport();
                        break;

                     // start
                     case 's':
                        importer.Start();
                        break;

                     // stop/exit
                     case 'c':
                        // exit on ctrl+c
                        if (key.Modifiers == ConsoleModifiers.Control)
                           Environment.Exit(0);

                        importer.Stop();
                        break;
                  }
               }
            });
}
