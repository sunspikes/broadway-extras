<?php

namespace Sunspikes\Broadway\Serialization;

use Broadway\Domain\AggregateRoot;
use Broadway\Domain\DomainEventStream;
use Broadway\Domain\DomainMessage;
use Broadway\Domain\Metadata;

trait SerializationAwareTrait
{
    use DeserializableTrait;

    /**
     * @return array
     */
    public function jsonSerialize()
    {
        $props = [];
        $propNames = self::getSerializableProperties();

        foreach ($propNames as $propName) {
            $props[$propName] = $this->$propName;
        }

        if (method_exists($this, 'getSerializableClearingProps')) {
            $props = array_merge($props, $this->getSerializableClearingProps());
        }

        return $props;
    }

    /**
     * @param array    $data
     * @param int|null $playhead
     * @return AggregateRoot
     */
    public static function createFromProps(array $data, int $playhead = null): AggregateRoot
    {
        $propNames = self::getSerializableProperties();
        $aggregateRoot = new static();

        if (method_exists($aggregateRoot, 'getSerializableClearingProps')) {
            $clearingProps = array_keys($aggregateRoot->getSerializableClearingProps());
            $propNames = array_merge($propNames, $clearingProps);
        }

        foreach ($propNames as $propName) {
            $value = $data[$propName];
            if (is_array($value) && !empty($value)) {
                $aggregateRoot->$propName = [];
                foreach ($value as $key => $item) {
                    $aggregateRoot->$propName[$key] = self::deserializeObject($item);
                }
            } else {
                $aggregateRoot->$propName = $value;
            }
        }

        self::movePlayhead($aggregateRoot, $playhead);

        return $aggregateRoot;
    }

    /**
     * @param AggregateRoot $aggregateRoot
     * @param int|null      $playhead
     * @return AggregateRoot
     */
    private static function movePlayhead(AggregateRoot $aggregateRoot, int $playhead = null): AggregateRoot
    {
        if (null !== $playhead) {
            $dummyMessage = DomainMessage::recordNow(1, 1, new Metadata([1]), new class {});
            $dummyStream = array_fill(0, $playhead + 1, $dummyMessage);
            $aggregateRoot->initializeState(new DomainEventStream($dummyStream));
        }

        return $aggregateRoot;
    }
}
