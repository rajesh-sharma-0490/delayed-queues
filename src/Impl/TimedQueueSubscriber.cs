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
        private int maxConcurrency;
        private string queueName;
        public TimedQueueSubscriber(ITimedQueueProvider timedQueueProvider, string queueName, Func<T, Task> callback, int maxConcurrency = 10)
        {
            this.queueProvider = timedQueueProvider;
            this.callback = callback;
            this.maxConcurrency = maxConcurrency;
            this.queueName = queueName;

            this.setupSubscription();
        }

        private void setupSubscription(){
            Func<Task> waitForNextTurn = null, processItemStream = null;
            
            processItemStream = async() => {
                while(true){
                    var items = await this.queueProvider.dequeueAvailableItemsAsync<T>(this.queueName, this.maxConcurrency);

                    if(this.isDisposed)
                        return;

                    items = items ?? new List<T>();
                    if(items.Count == 0){
                        await waitForNextTurn();
                        continue;
                    }
                    
                    var tasks = items.Select(x => this.callback(x));

                    await Task.WhenAll(tasks);
                }
            };

            waitForNextTurn = async() => {
                var timestamp = await this.queueProvider.getNextItemTimestampAsync(this.queueName);

                TimeSpan delay;
                if(timestamp != null && timestamp.HasValue)
                    delay = timestamp.Value.ToUniversalTime() - DateTime.Now.ToUniversalTime();
                else
                    delay = TimeSpan.FromMinutes(5);

                await Task.Delay(delay);
            };
            
            Task.Run(() => processItemStream());
        }

        public void Dispose(){
            if(this.isDisposed)
                return;
        }
    }
}