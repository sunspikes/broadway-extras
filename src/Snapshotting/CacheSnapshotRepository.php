<?php

namespace Sunspikes\Broadway\Snapshotting;

use Assert\Assertion;
use Broadway\Snapshotting\Snapshot\Snapshot;
use Broadway\Snapshotting\Snapshot\SnapshotRepository;
use Doctrine\Common\Cache\CacheProvider;
use Sunspikes\Broadway\Serialization\SerializableAggregateInterface;

class CacheSnapshotRepository implements SnapshotRepository
{
    const CACHE_NAMESPACE = 'xl:snapshot:';

    /**
     * @var CacheProvider
     */
    private $cache;

    /**
     * @param CacheProvider $cache
     */
    public function __construct(CacheProvider $cache)
    {
        $cache->setNamespace(self::CACHE_NAMESPACE);
        $this->cache = $cache;
    }

    /**
     * @param mixed $id should be unique across aggregate types
     * @return Snapshot|null
     * @throws \Assert\AssertionFailedException
     */
    public function load($id): ?Snapshot
    {
        $result = $this->cache->fetch($id);

        if (false !== $result) {
            $data = json_decode($result, true);
            $aggregate = $data['aggregate'];
            $playhead = $data['playhead'];
            $className = $data['type'];

            Assertion::implementsInterface(
                $className,
                SerializableAggregateInterface::class,
                'The aggregate must implement "SerializableAggregateInterface" interface.'
            );
            $aggregateRoot = $className::createFromProps($aggregate, $playhead);

            return new Snapshot($aggregateRoot);
        }

        return null;
    }

    /**
     * @param Snapshot $snapshot
     * @throws \Assert\AssertionFailedException
     */
    public function save(Snapshot $snapshot): void
    {
        $aggregate = $snapshot->getAggregateRoot();
        $id = $aggregate->getAggregateRootId();
        $playhead = $aggregate->getPlayhead();
        $type = \get_class($aggregate);

        Assertion::implementsInterface(
            $type,
            SerializableAggregateInterface::class,
            'The aggregate must implement "SerializableAggregateInterface" interface.'
        );

        $this->cache->save(
            $id,
            json_encode(
                compact('id', 'aggregate', 'playhead', 'type')
            )
        );
    }
}
