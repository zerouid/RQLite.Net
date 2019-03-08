using System;
using System.Threading;
using System.Threading.Tasks;
using Rafty.Infrastructure;
using RQLite.Net.Store;

namespace RQLite.Net.Util
{
    public static class TaskExtensions
    {
        public static async Task<TResult> TimeoutAfter<TResult>(this Task<TResult> task, TimeSpan timeout, Exception timeoutException = null)
        {
            using (var timeoutCancellationTokenSource = new CancellationTokenSource())
            {
                var completedTask = await Task.WhenAny(task, Task.Delay(timeout, timeoutCancellationTokenSource.Token));
                if (completedTask == task)
                {
                    timeoutCancellationTokenSource.Cancel();
                    return await task;  // Very important in order to propagate exceptions
                }
                else
                {
                    throw timeoutException ?? new TimeoutException("The operation has timed out.");
                }
            }
        }

        public static async Task<Response<TResult>> ThrowOnError<TResult>(this Task<Response<TResult>> task)
        {
            var result = await task;
            if (result is ErrorResponse<TResult>)
            {
                throw new RaftException<TResult>(result as ErrorResponse<TResult>);
            }
            else
            {
                return result;
            }
        }
    }
}