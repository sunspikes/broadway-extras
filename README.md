Broadway Extras
================

Extras for broadway CQRS / event sourcing library

## Installation

```
composer require sunspikes/broadway-extras
```

## What's inside?

[Saga](/src/Saga)

StateManager: Use this to have multiple saga creation policies as defined in `CreationPolicy` or define custom creation policy with a closure callback `creation_policy_callback`

DBALSagaRepository: DBAL driver for sagas

DynamoDBSagaRepository: DynamoDB driver for sagas

[Serialization](/src/Serialization)

SerializableAggregateInterface & SerializationAwareTrait: Make aggregates serializable (so that it can be snapshotted)

[Snapshotting](/src/Snapshotting)

CachingSnapshotEventSourcingRepository: Adds a caching layer to snapshots, so load will get the aggregate in the order `Static Cache -> Cache -> Snapshot -> Events`

CacheSnapshotRepository & RedisCacheFactory: Snapshot the aggregates on in redis (or other cache stores)

DynamoDBEventStore: DynamoDB driver for event store

## TODO

_Detailed usage & Integrations with frameworks guide_

## Author

Krishnaprasad MG [@sunspikes]

_Contact me at [sunspikes at gmail dot com]_
