<?php

namespace Sunspikes\Broadway\Saga;

use Aws\DynamoDb\DynamoDbClient;
use Aws\DynamoDb\Marshaler;
use Broadway\Saga\State;
use Broadway\Saga\State\Criteria;
use Broadway\Saga\State\RepositoryException;
use Broadway\Saga\State\RepositoryInterface;

class DynamoDBSagaRepository implements RepositoryInterface
{
    /**
     * @var DynamoDbClient
     */
    private $dynamoDbClient;

    /**
     * @var string
     */
    private $tableName;

    /**
     * @param DynamoDbClient $dynamoDbClient
     * @param string         $tableName
     */
    public function __construct(DynamoDbClient $dynamoDbClient, $tableName)
    {
        $this->dynamoDbClient = $dynamoDbClient;
        $this->tableName = $tableName;
    }

    /**
     * {@inheritDoc}
     */
    public function findOneBy(Criteria $criteria, $sagaId)
    {
        $marshaller = new Marshaler();
        $sagas = $this->dynamoDbClient
            ->getIterator('Scan', $this->createQuery($criteria, $sagaId));

        $count = \count($sagas);

        if (null !== $sagas->current()) {
            if ($count === 1) {
                $saga = $marshaller->unmarshalItem($sagas->current());

                return State::deserialize($saga);
            }

            if ($count > 1) {
                throw new RepositoryException('Multiple saga state instances found.');
            }
        }

        return null;
    }

    /**
     * {@inheritDoc}
     */
    public function save(State $state, $sagaId)
    {
        $marshaller = new Marshaler();
        $serializedState = $state->serialize();
        $serializedState['_id'] = $serializedState['id'];
        $serializedState['sagaId'] = $sagaId;
        $serializedState['removed'] = (int) $state->isDone();
        $serializedState['done'] = (int) $state->isDone();

        $this->dynamoDbClient->putItem([
            'TableName' => $this->tableName,
            'Item'      => $marshaller->marshalItem($serializedState),
        ]);
    }

    /**
     * Create the SagaRepository table in DynamoDB
     */
    public function createEventStoreTable()
    {
        $this->dynamoDbClient->createTable([
            'TableName'             => $this->tableName,
            'AttributeDefinitions'  => [
                ['AttributeName' => 'sagaId', 'AttributeType' => 'S'],
                ['AttributeName' => 'removed', 'AttributeType' => 'N'],
            ],
            'KeySchema'             => [
                ['AttributeName' => 'sagaId', 'KeyType' => 'HASH'],
                ['AttributeName' => 'removed', 'KeyType' => 'RANGE'],
            ],
            'ProvisionedThroughput' => [
                'ReadCapacityUnits'  => 10,
                'WriteCapacityUnits' => 20,
            ],
        ]);
    }

    /**
     * @param Criteria $criteria
     * @param string   $sagaId
     * @return array
     */
    private function createQuery(Criteria $criteria, string $sagaId): array
    {
        $comparisons = $criteria->getComparisons();

        $marshaller = new Marshaler();
        $query = [
            'TableName'      => $this->tableName,
            'ConsistentRead' => true,
            'KeyConditions'  => [
                'sagaId'  => [
                    'AttributeValueList' => [
                        $marshaller->marshalValue($sagaId),
                    ],
                    'ComparisonOperator' => 'EQ',
                ],
                'removed' => [
                    'AttributeValueList' => [
                        $marshaller->marshalValue(0),
                    ],
                    'ComparisonOperator' => 'EQ',
                ],
            ],
        ];

        foreach ($comparisons as $key => $value) {
            $query['ScanFilter']['values.'.$key] = [
                'AttributeValueList' => [
                    $marshaller->marshalValue($value),
                ],
                'ComparisonOperator' => 'EQ',
            ];
        }

        return $query;
    }
}
