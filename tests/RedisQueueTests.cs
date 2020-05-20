using System.Diagnostics;
using NUnit.Framework;
using DelayedQueues.Impl;
using System.Threading.Tasks;
using System;
using System.Collections.Generic;

namespace DelayedQueuesTests
{
    public class RedisQueueTests
    {
        private string queueName = "testQueue";
        private Model model = new Model(){Name = "Kratos"};

        private RedisTimedQueueProvider provider =  new RedisTimedQueueProvider();

        [Test]
        public async Task TestImmediateItems()
        {
            await provider.enqueueItemAsync(queueName, model, TimeSpan.Zero);

            var items = await provider.dequeueAvailableItemsAsync<Model>(queueName);

            Assert.AreEqual(items.Count, 1);

            Assert.AreEqual(items[0].Name, model.Name);
        }

        [Test]
        public async Task TestDelayedItems()
        {
            await provider.enqueueItemAsync(queueName, model, TimeSpan.FromSeconds(3));

            var items = await provider.dequeueAvailableItemsAsync<Model>(queueName);

            Assert.AreEqual(items.Count, 0);

            await Task.Delay(TimeSpan.FromSeconds(3));

            items = await provider.dequeueAvailableItemsAsync<Model>(queueName);

            Assert.AreEqual(items.Count, 1);

            Assert.AreEqual(items[0].Name, model.Name);
        }

        [Test]
        public async Task TestGetNextItemTimestamp(){
            var timer = new Stopwatch();
            timer.Start();
            await provider.enqueueItemAsync(queueName, model, TimeSpan.FromSeconds(30));

            var timestamp = await provider.getNextItemTimestampAsync(queueName);
            await provider.dequeueAvailableItemsAsync<Model>(queueName);

            timer.Stop();

            Assert.IsNotNull(timestamp);
            Assert.IsTrue(timestamp.HasValue);

            var diff = timestamp.Value.ToUniversalTime().Subtract((DateTime.Now - timer.Elapsed).ToUniversalTime()).Seconds;

            Assert.True(Math.Abs(30 - diff) <= 1);
        }

        [Test]
        public async Task TestSubscriber(){
            var queueName = "pubsub";
            var models = new List<Model>(){new Model(){Name = "Hades"}, new Model(){Name = "Poseidon"}};
            var consumed = new List<Model>();
            provider.subscribe(queueName, (Model item) => {
                consumed.Add(item);
                return Task.CompletedTask;
            });

            await provider.enqueueItemAsync(queueName, models[0], TimeSpan.FromSeconds(1));
            await provider.enqueueItemAsync(queueName, models[1], TimeSpan.Zero);

            await Task.Delay(TimeSpan.FromSeconds(2));

            Assert.IsNotEmpty(consumed);
            Assert.IsTrue(consumed.Find(x => x.Name == models[0].Name) != null);
            Assert.IsTrue(consumed.Find(x => x.Name == models[1].Name) != null);
        }
    }

    internal class Model{
        public String Name { get; set; }
    }
}