<?php

namespace Sunspikes\Broadway\Snapshotting;

use Assert\Assertion;
use Broadway\Snapshotting\Snapshot\Snapshot;
use Broadway\Snapshotting\Snapshot\SnapshotRepository;
use Doctrine\DBAL\Connection;
use Sunspikes\Broadway\Serialization\SerializableAggregateInterface;

class DBALSnapshotRepository implements SnapshotRepository
{
    /**
     * @var Connection
     */
    private $connection;
    /**
     * @var string
     */
    private $tableName;

    /**
     * @param Connection $connection
     * @param string     $tableName
     */
    public function __construct(Connection $connection, string $tableName)
    {
        $this->connection = $connection;
        $this->tableName = $tableName;
    }

    /**
     * @param mixed $id should be unique across aggregate types
     * @return Snapshot|null
     * @throws \Assert\AssertionFailedException
     */
    public function load($id)
    {
        $result = $this->connection
            ->fetchAssoc("SELECT `id`, `aggregate`, `playhead`, `type` FROM {$this->tableName} WHERE id = ?", [$id]);

        if (false !== $result) {
            $data = igbinary_unserialize($result['aggregate']);
            $playhead = $result['playhead'];
            $className = $result['type'];

            Assertion::implementsInterface(
                $className,
                SerializableAggregateInterface::class,
                'The aggregate must implement "SerializableAggregateInterface" interface.'
            );
            $aggregateRoot = $className::createFromProps($data, $playhead);

            return new Snapshot($aggregateRoot);
        }

        return null;
    }

    /**
     * @param Snapshot $snapshot
     * @throws \Assert\AssertionFailedException
     */
    public function save(Snapshot $snapshot)
    {
        $aggregateRoot = $snapshot->getAggregateRoot();
        $id = $aggregateRoot->getAggregateRootId();
        $playhead = $aggregateRoot->getPlayhead();
        $className = get_class($aggregateRoot);

        Assertion::implementsInterface(
            $className,
            SerializableAggregateInterface::class,
            'The aggregate must implement "SerializableAggregateInterface" interface.'
        );
        $serializedAggregate = igbinary_serialize($aggregateRoot);

        $existingSnapshot = $this->connection
            ->fetchAssoc("SELECT `id` FROM {$this->tableName} WHERE id = ?", [$id]);

        if ($existingSnapshot) {
            $this->connection
                ->update(
                    $this->tableName,
                    [
                        'aggregate' => $serializedAggregate,
                        'playhead'  => $playhead,
                    ],
                    [
                        'id' => $id,
                    ]
                );
        } else {
            $this->connection
                ->insert(
                    $this->tableName,
                    [
                        'id'        => $id,
                        'aggregate' => $serializedAggregate,
                        'playhead'  => $playhead,
                        'type'      => $className,
                    ]
                );
        }
    }
}
