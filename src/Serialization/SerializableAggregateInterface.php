<?php

namespace Sunspikes\Broadway\Serialization;

use Broadway\Domain\AggregateRoot;

interface SerializableAggregateInterface extends \JsonSerializable
{
    /**
     * Array of serializable aggregate properties
     *
     * @return array
     */
    public static function getSerializableProperties(): array;

    /**
     * Create the aggregate from properties and values
     *
     * @param array    $data
     * @param int|null $playhead
     * @return AggregateRoot
     */
    public static function createFromProps(array $data, int $playhead = null): AggregateRoot;
}
