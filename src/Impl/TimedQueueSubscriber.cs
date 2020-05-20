using System.Threading.Tasks;
using System;
using DelayedQueues.Contracts;
using System.Collections.Generic;
using System.Linq;

namespace DelayedQueues.Impl
{
    public class TimedQueueSubscriber<T> : ITimedQueueSubscriber<T>
    {
        private ITimedQueueProvider queueProvider;
        private Func<T, Task> callback;
        private bool isDisposed = false;
        private int availableSlots;
        private string queueName;

        private List<Task> itemsInExecution = new List<Task>();
        public TimedQueueSubscriber(ITimedQueueProvider timedQueueProvider, string queueName, Func<T, Task> callback, int maxConcurrency = 10)
        {
            this.queueProvider = timedQueueProvider;
            this.callback = callback;
            this.availableSlots = maxConcurrency;
            this.queueName = queueName;

            Task.Run(() => this.processItemsAsync());
        }


        private async Task<IList<T>> getMoreItemsAsync(){
            while(true){
                var items = await this.queueProvider.dequeueAvailableItemsAsync<T>(this.queueName, this.availableSlots);

                items = items ?? new List<T>();

                if(items.Count > 0)
                    return items;

                await this.waitForNextTurnAsync();
            }
        }

        private async Task waitForNextTurnAsync(){
            var timestamp = await this.queueProvider.getNextItemTimestampAsync(this.queueName);

            TimeSpan delay;
            if(timestamp != null && timestamp.HasValue)
                delay = timestamp.Value.ToUniversalTime() - DateTime.Now.ToUniversalTime();
            else
                delay = TimeSpan.FromMinutes(5);

            await Task.Delay(delay);
        }


        private async Task processItemsAsync(){
            while(true){
                var items = await this.getMoreItemsAsync();
                this.availableSlots -= items.Count;

                if(this.isDisposed){
                    await Task.WhenAll(this.itemsInExecution);
                    break;
                }

                this.itemsInExecution.AddRange(items.Select(this.callback));

                var finished = await Task.WhenAny(this.itemsInExecution.ToArray());

                this.itemsInExecution.Remove(finished);

                this.availableSlots++;
            }
        }

        public void Dispose(){
            if(this.isDisposed)
                throw new InvalidOperationException("Already disposed");

            this.isDisposed = true;
        }
    }
}