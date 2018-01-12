<?php

namespace Sunspikes\Broadway\Snapshotting;

use Broadway\Domain\AggregateRoot;
use Broadway\EventSourcing\EventSourcingRepository;
use Broadway\EventStore\EventStore;
use Broadway\Repository\Repository;
use Broadway\Snapshotting\Snapshot\Snapshot;
use Broadway\Snapshotting\Snapshot\SnapshotNotFoundException;
use Broadway\Snapshotting\Snapshot\SnapshotRepository;
use Broadway\Snapshotting\Snapshot\Trigger;
use Doctrine\Common\Cache\CacheProvider;

class CachingSnapshotEventSourcingRepository implements Repository
{
    private $eventSourcingRepository;
    private $eventStore;
    private $snapshotRepository;
    private $trigger;
    private $cache;
    private $staticCache;

    /**
     * @param EventSourcingRepository $eventSourcingRepository
     * @param EventStore              $eventStore
     * @param SnapshotRepository      $snapshotRepository
     * @param Trigger                 $trigger
     * @param CacheProvider           $cache
     */
    public function __construct(
        EventSourcingRepository $eventSourcingRepository,
        EventStore $eventStore,
        SnapshotRepository $snapshotRepository,
        Trigger $trigger,
        CacheProvider $cache
    ) {
        $this->eventSourcingRepository = $eventSourcingRepository;
        $this->eventStore              = $eventStore;
        $this->snapshotRepository      = $snapshotRepository;
        $this->trigger                 = $trigger;
        $this->cache                   = $cache;
        $this->staticCache             = [];
    }

    /**
     * {@inheritdoc}
     */
    public function load($id): AggregateRoot
    {
        // 1. Check in static cache
        if (isset($this->staticCache[$id])) {
            return $this->staticCache[$id];
        }

        // 2. Check in cache
        if (true === $this->cache->contains($id)) {
            return $this->cache->fetch($id);
        }

        // 3. Check for snapshot
        $snapshot = $this->snapshotRepository->load($id);

        // 4. Rebuild the aggregate
        if (null === $snapshot) {
            return $this->eventSourcingRepository->load($id);
        }

        $aggregateRoot = $snapshot->getAggregateRoot();
        $aggregateRoot->initializeState(
            $this->eventStore->loadFromPlayhead($id, $snapshot->getPlayhead() + 1)
        );

        return $aggregateRoot;
    }

    /**
     * {@inheritdoc}
     */
    public function save(AggregateRoot $aggregate)
    {
        $takeSnaphot = $this->trigger->shouldSnapshot($aggregate);

        // 1. Save to the ES repo
        $this->eventSourcingRepository->save($aggregate);

        // 2. Save the snapshot
        if ($takeSnaphot) {
            $this->snapshotRepository->save(
                new Snapshot($aggregate)
            );
        }

        // 3. Save in cache
        $this->cache->save($aggregate->getAggregateRootId(), $aggregate);

        // 4. Save in static cache
        $this->staticCache[$aggregate->getAggregateRootId()] = $aggregate;
    }
}
