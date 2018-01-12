<?php

namespace Sunspikes\Broadway\Saga;

use Broadway\Saga\State;
use Broadway\Saga\State\Criteria;
use Broadway\Saga\State\RepositoryException;
use Broadway\Saga\State\RepositoryInterface;
use Doctrine\DBAL\Driver\Connection;
use PDO;

class DBALSagaRepository implements RepositoryInterface
{
    /**
     * @var Connection
     */
    private $connection;

    /** @var string */
    private $table;

    /**
     * @param Connection $connection
     * @param string     $table
     */
    public function __construct(Connection $connection, $table)
    {
        $this->connection = $connection;
        $this->table = $table;
    }

    /**
     * @param Criteria $criteria
     * @param string   $sagaId
     * @return State
     */
    public function findOneBy(Criteria $criteria, $sagaId)
    {
        $results = $this->getQueryResult($criteria, $sagaId);
        $count   = \count($results);

        if ($count === 1) {
            $result = current($results);
            $result['id'] = $result['id'];
            $result['done'] = (int) $result['removed'];
            $result['values'] = json_decode($result['values'], true);

            return State::deserialize($result);
        }

        if ($count > 1) {
            throw new RepositoryException('Multiple saga state instances found.');
        }

        return null;
    }

    /**
     * @param State  $state
     * @param string $sagaId
     * @throws \Exception
     */
    public function save(State $state, $sagaId)
    {
        $serializedState = $state->serialize();
        $saveState['id'] = $serializedState['id'];
        $saveState['saga_id'] = $sagaId;
        $saveState['removed'] = (int) $state->isDone();
        $saveState['`values`'] = json_encode($serializedState['values']);

        $id = $this->activeSagaExists($sagaId, $state->getId());
        $this->connection->beginTransaction();

        try {
            if (false === $id) {
                $this->connection->insert($this->table, $saveState);
            } else {
                $this->connection->update($this->table, $saveState, ['id' => $id]);
            }
            $this->connection->commit();
        } catch (\Exception $e) {
            $this->connection->rollBack();

            throw $e;
        }
    }

    /**
     * @param Criteria $criteria
     * @param string   $sagaId
     * @return array
     */
    private function getQueryResult(Criteria $criteria, string $sagaId)
    {
        $comparisons = $criteria->getComparisons();
        $selects     = ['id', 'saga_id', 'removed', '`values`'];

        $query = 'SELECT '. implode(', ', $selects) .' FROM '. $this->table .' WHERE removed = ? AND saga_id = ?';

        $params = [0, $sagaId];
        foreach ($comparisons as $key => $value) {
            $params[] = $value;
            $query .= ' AND JSON_EXTRACT(`values`, \'$.'.$key.'\') = ?';
        }

        $types = $this->getParamTypes($params);
        $result = $this->connection->fetchAll($query, $params, $types);

        return $result;
    }

    /**
     * @param array $params
     * @return array
     */
    private function getParamTypes(array $params): array
    {
        $supportedTypes = [
            'NULL' => PDO::PARAM_NULL,
            'integer' => PDO::PARAM_INT,
            'string' => PDO::PARAM_STR,
            'boolean' => PDO::PARAM_BOOL,
        ];

        return array_map(function ($param) use ($supportedTypes) {
            return $supportedTypes[\gettype($param)] ?? PDO::PARAM_STR;
        }, $params);
    }

    /**
     * @param string $sagaId
     * @param string $id
     * @return string|bool
     */
    private function activeSagaExists($sagaId, $id)
    {
        $query = 'SELECT 1 FROM '. $this->table .' WHERE saga_id = ? AND id = ? AND removed = ?';
        $params = [$sagaId, $id, 0];

        $results = $this->connection->fetchAll($query, $params);

        if ($results) {
            return $id;
        }

        return false;
    }
}
