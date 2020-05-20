using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace DelayedQueues.Contracts
{
    public interface ITimedQueueProvider{
        Task enqueueItemAsync<T>(string queueName, T item, TimeSpan delay);

        Task<IList<T>> dequeueAvailableItemsAsync<T>(string queueName, int maxItems = 10);

        Task<DateTime?> getNextItemTimestampAsync(string queueName);

        ITimedQueueSubscriber<T> subscribe<T>(string queueName, Func<T, Task> callback, int maxConcurrency = 10);
    }
}