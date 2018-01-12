<?php

namespace Sunspikes\Broadway;

use Aws\DynamoDb\BinaryValue;
use Aws\DynamoDb\DynamoDbClient;
use Aws\DynamoDb\Exception\DynamoDbException;
use Aws\DynamoDb\Marshaler;
use Broadway\Domain\DateTime;
use Broadway\Domain\DomainMessage;
use Broadway\Serializer\Serializer;
use Broadway\Domain\DomainEventStream;
use Broadway\EventStore\EventStore;
use Broadway\EventStore\EventVisitor;
use Broadway\EventStore\Management\Criteria;
use Broadway\EventStore\EventStreamNotFoundException;
use Broadway\EventStore\Management\EventStoreManagement;
use Broadway\EventStore\Exception\DuplicatePlayheadException;
use Broadway\EventStore\Management\CriteriaNotSupportedException;

class DynamoDBEventStore implements EventStore, EventStoreManagement
{
    /**
     * @var DynamoDbClient
     */
    private $dynamoDbClient;

    /**
     * @var Serializer
     */
    private $payloadSerializer;

    /**
     * @var Serializer
     */
    private $metadataSerializer;

    /**
     * @var string
     */
    private $tableName;

    /**
     * DynamoDbEventStore constructor.
     * @param DynamoDbClient $dynamoDbClient
     * @param Serializer     $payloadSerializer
     * @param Serializer     $metadataSerializer
     * @param string         $tableName
     */
    public function __construct(
        DynamoDbClient $dynamoDbClient,
        Serializer $payloadSerializer,
        Serializer $metadataSerializer,
        $tableName
    ) {
        $this->dynamoDbClient = $dynamoDbClient;
        $this->payloadSerializer = $payloadSerializer;
        $this->metadataSerializer = $metadataSerializer;
        $this->tableName = $tableName;
    }

    /**
     * {@inheritdoc}
     */
    public function load($id): DomainEventStream
    {
        $cursor = $this->dynamoDbClient
            ->getIterator('Query', $this->buildLoadQuery($id, '0'));

        $domainMessages = [];

        foreach ($cursor as $domainMessage) {
            $domainMessages[] = $this->denormalizeDomainMessage($domainMessage);
        }

        if (empty($domainMessages)) {
            throw new EventStreamNotFoundException(sprintf('EventStream not found for aggregate with id %s', (string) $id));
        }

        return new DomainEventStream($domainMessages);
    }

    /**
     * {@inheritdoc}
     */
    public function loadFromPlayhead($id, $playhead): DomainEventStream
    {
        $cursor = $this->dynamoDbClient
            ->getIterator('Query', $this->buildLoadQuery($id, $playhead));

        $domainMessages = [];

        foreach ($cursor as $domainMessage) {
            $domainMessages[] = $this->denormalizeDomainMessage($domainMessage);
        }

        return new DomainEventStream($domainMessages);
    }


    /**
     * {@inheritdoc}
     */
    public function append($id, DomainEventStream $eventStream)
    {
        $messages['RequestItems'][$this->tableName] = [];

        foreach ($eventStream as $message) {
            $messages['RequestItems'][$this->tableName][] = $this->normalizeDomainMessage($message);
        }

        try {
            if (!empty($messages['RequestItems'][$this->tableName])) {
                $this->dynamoDbClient->batchWriteItem($messages);
            }
        } catch (DynamoDbException $dynamoDbException) {
            throw new DuplicatePlayheadException($eventStream, $dynamoDbException);
        }
    }

    /**
     * {@inheritdoc}
     * @throws \Broadway\EventStore\Management\CriteriaNotSupportedException
     */
    public function visitEvents(Criteria $criteria, EventVisitor $eventVisitor)
    {
        if ($criteria->getAggregateRootTypes()) {
            throw new CriteriaNotSupportedException(
                'DynamoDB implementation cannot support criteria based on aggregate root types.'
            );
        }

        $events = $this->dynamoDbClient
            ->getIterator('Scan', $this->buildScanByCriteria($criteria));

        foreach ($events as $event) {
            $eventVisitor->doWithEvent($this->denormalizeDomainMessage($event));
        }
    }

    /**
     * Create the EventStore table in DynamoDB
     */
    public function createEventStoreTable()
    {
        $this->dynamoDbClient->createTable([
            'TableName' => $this->tableName,
            'AttributeDefinitions' => [
                ['AttributeName' => 'uuid', 'AttributeType' => 'S'],
                ['AttributeName' => 'playhead', 'AttributeType' => 'N'],
            ],
            'KeySchema' => [
                ['AttributeName' => 'uuid', 'KeyType' => 'HASH'],
                ['AttributeName' => 'playhead', 'KeyType' => 'RANGE'],
            ],
            'LocalSecondaryIndexes' => [
                [
                    'IndexName' => 'UuidPlayheadIndex',
                    'KeySchema' => [
                        ['AttributeName' => 'uuid', 'KeyType' => 'HASH'],
                        ['AttributeName' => 'playhead',  'KeyType' => 'RANGE'],
                    ],
                    'Projection' => [
                        'ProjectionType' => 'KEYS_ONLY',
                    ],
                ],
            ],
            'ProvisionedThroughput' => [
                'ReadCapacityUnits'  => 10,
                'WriteCapacityUnits' => 20,
            ],
        ]);
    }

    /**
     * @param string $id
     * @param string $playhead
     *
     * @return array
     */
    private function buildLoadQuery(string $id, string $playhead): array
    {
        return [
            'TableName'        => $this->tableName,
            'AttributesToGet'  => ['uuid', 'playhead', 'type', 'metadata', 'payload', 'recorded_on'],
            'IndexName'        => 'UuidPlayheadIndex',
            'ConsistentRead'   => true,
            'ScanIndexForward' => true,
            'KeyConditions' => [
                'uuid' => [
                    'AttributeValueList' => [
                        ['S' => $id],
                    ],
                    'ComparisonOperator' => 'EQ',
                ],
                'playhead' => [
                    'AttributeValueList' => [
                        ['N' => $playhead],
                    ],
                    'ComparisonOperator' => 'GE',
                ],
            ],
        ];
    }

    /**
     * @param \Iterator $event
     *
     * @return DomainMessage
     */
    private function denormalizeDomainMessage($event): DomainMessage
    {
        $marshaller = new Marshaler();
        $item = $marshaller->unmarshalItem($event);

        return new DomainMessage(
            $item['uuid'],
            $item['playhead'],
            $this->metadataSerializer->deserialize(json_decode((string) $item['metadata'], true)),
            $this->payloadSerializer->deserialize(json_decode((string) $item['payload'], true)),
            DateTime::fromString($item['recorded_on'])
        );
    }

    /**
     * @param DomainMessage $message
     *
     * @return array
     */
    private function normalizeDomainMessage(DomainMessage $message): array
    {
        $marshaller = new Marshaler();

        return [
            'PutRequest' => [
                'Item' => $marshaller->marshalItem([
                    'uuid'        => $message->getId(),
                    'playhead'    => $message->getPlayhead(),
                    'metadata'    => new BinaryValue(json_encode($this->metadataSerializer->serialize($message->getMetadata()))),
                    'payload'     => new BinaryValue(json_encode($this->payloadSerializer->serialize($message->getPayload()))),
                    'recorded_on' => $message->getRecordedOn()->toString(),
                    'type'        => $message->getType(),
                ]),
            ],
        ];
    }

    /**
     * @param Criteria $criteria
     * @return array
     */
    private function buildScanByCriteria(Criteria $criteria): array
    {
        $marshaller = new Marshaler();
        $query = [
            'TableName'        => $this->tableName,
            'AttributesToGet'  => ['uuid', 'playhead', 'type', 'metadata', 'payload', 'recorded_on'],
            'IndexName'        => 'UuidPlayheadIndex',
            'ConsistentRead'   => true,
            'ScanIndexForward' => true,
        ];

        if ($criteria->getAggregateRootIds()) {
            $query['ScanFilter']['uuid'] = [
                'AttributeValueList' => $marshaller->marshalItem($criteria->getAggregateRootIds()),
                'ComparisonOperator' => 'IN',
            ];
        }

        if ($criteria->getEventTypes()) {
            $query['ScanFilter']['type'] = [
                'AttributeValueList' => $marshaller->marshalItem($criteria->getEventTypes()),
                'ComparisonOperator' => 'IN',
            ];
        }

        return $query;
    }
}
