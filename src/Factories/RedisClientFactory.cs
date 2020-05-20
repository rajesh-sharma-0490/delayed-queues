using System;
using StackExchange.Redis;

namespace DelayedQueues.Factories{
    internal static class RedisClientFactory{
        private static readonly Object lockAnchor = new Object();

        private static ConnectionMultiplexer connectionMultiplexer;

        public static IDatabase getInstance(){
            Func<IDatabase> getDb = () => connectionMultiplexer.GetDatabase(0);
            
            if(connectionMultiplexer != null)
                return getDb();

            lock(lockAnchor){
                connectionMultiplexer = connectionMultiplexer ?? ConnectionMultiplexer.Connect("localhost");
            }

            return getDb();
        }
    }
}