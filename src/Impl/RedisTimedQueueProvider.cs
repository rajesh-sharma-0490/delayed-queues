using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using DelayedQueues.Contracts;
using DelayedQueues.Factories;
using Newtonsoft.Json;
using StackExchange.Redis;
using System.Linq;

namespace DelayedQueues.Impl
{
    public class RedisTimedQueueProvider : ITimedQueueProvider{
        public async Task<IList<T>> dequeueAvailableItemsAsync<T>(string queueName, int maxItems = 10)
        {
            var client = this.getRedisDb();
            var transaction = client.CreateTransaction();

            var queueKey = new RedisKey(queueName);
            var currentEpoch = this.getTimestamp(TimeSpan.Zero);

            var itemsTask = transaction.SortedSetRangeByScoreAsync(queueKey, double.NegativeInfinity, currentEpoch,
                Exclude.None, Order.Ascending, 0, maxItems);

            var deletionTask = transaction.SortedSetRemoveRangeByScoreAsync(queueKey, double.NegativeInfinity, currentEpoch, Exclude.None);
            
            if(await transaction.ExecuteAsync()){
                var items = await itemsTask;
                await deletionTask;

                return (items ?? Array.Empty<RedisValue>()).ToStringArray()
                    .Select(x => JsonConvert.DeserializeObject<T>(x)).ToList();
            }

            return new List<T>();
        }

        public async Task enqueueItemAsync<T>(string queueName, T item, TimeSpan delay)
        {
            var client = this.getRedisDb();
            var value = JsonConvert.SerializeObject(item);
            await client.SortedSetAddAsync(new RedisKey(queueName), new RedisValue(value), this.getTimestamp(delay));
        }

        public async Task<DateTime?> getNextItemTimestampAsync(string queueName){
            var client = this.getRedisDb();
            var items = await client.SortedSetRangeByScoreWithScoresAsync(new RedisKey(queueName), double.PositiveInfinity, double.NegativeInfinity, Exclude.None,
                Order.Descending, 0, 1);

            if((items ?? Array.Empty<SortedSetEntry>()).Length == 0)
                return null;

            return DateTimeOffset.FromUnixTimeSeconds(long.Parse(items[0].Score.ToString())).UtcDateTime;
        }

        public ITimedQueueSubscriber<T> subscribe<T>(string queueName, Func<T, Task> callback, int maxConcurrency = 10){
            return new TimedQueueSubscriber<T>(this, queueName, callback, maxConcurrency);
        }

        private IDatabase getRedisDb(){
            return RedisClientFactory.getInstance();
        }

        private double getTimestamp(TimeSpan delay){
            return new DateTimeOffset(DateTime.Now.Add(delay).ToUniversalTime()).ToUnixTimeSeconds();
        }
    }
}