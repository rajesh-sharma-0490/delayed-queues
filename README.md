# delayed-queues

## Redis based delayed queue implemention with following features:

1. Publish an item to the queue with a custom delay interval.
2. The queue allows consuming currently available items with a max batch size.
3. There's also a consumer that allows subscribing to a queue and emits items as they start becoming available with a specification of the max level of concurrency.