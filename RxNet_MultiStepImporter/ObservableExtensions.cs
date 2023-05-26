using System.Reactive;
using System.Reactive.Linq;

namespace RxNet_MultiStepImporter;

public static class ObservableExtensions
{
   public static IObservable<IObservable<TResult>> SelectAsync<TSource, TResult>(
      this IObservable<TSource> source,
      Func<TSource, CancellationToken, Task<TResult>> selector)
   {
      return source.Select(value => Observable.FromAsync(token => selector(value, token)));
   }

   public static IObservable<Unit> SelectManyAsync<TSource>(
      this IObservable<TSource> source,
      Func<TSource, CancellationToken, Task> selector)
   {
      return source.SelectMany(value => Observable.FromAsync(token => selector(value, token)));
   }

   public static IObservable<TResult> SelectThrottle<TSource, TResult>(
      this IObservable<TSource> source,
      Func<TSource, IObservable<TResult>> selector,
      int maxConcurrency = 1,
      bool appendAnotherRunIfEmittedDuringExecution = true)
   {
      if (source == null)
         throw new ArgumentNullException(nameof(source));
      if (selector == null)
         throw new ArgumentNullException(nameof(selector));
      if (maxConcurrency < 1)
         throw new ArgumentOutOfRangeException(nameof(maxConcurrency), "The concurrency cannot be less than 1.");

      return Observable.Defer(() =>
      {
         var innerConcurrency = appendAnotherRunIfEmittedDuringExecution && maxConcurrency < Int32.MaxValue
                                   ? maxConcurrency + 1
                                   : maxConcurrency;
         var concurrentCalls = 0;

         return source.Select(v =>
         {
            if (Interlocked.Increment(ref concurrentCalls) <= innerConcurrency)
               return selector(v).Do(DoNoting, () => Interlocked.Decrement(ref concurrentCalls));

            Interlocked.Decrement(ref concurrentCalls);

            return Observable.Empty<TResult>();
         });
      })
     .Merge(maxConcurrency);
   }

   private static void DoNoting<T>(T item)
   {
   }
}
