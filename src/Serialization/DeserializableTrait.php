<?php

namespace Sunspikes\Broadway\Serialization;

trait DeserializableTrait
{
    /**
     * @param array|string $object
     * @return object
     */
    public static function deserializeObject($object)
    {
        if (is_array($object) && !empty($object)) {
            $class = key($object);
            $params = $object[$class];

            if (is_array($params)) {
                $params = array_map([static::class, 'deserializeObject'], $params);
            } else {
                $params = [$params];
            }

            $params = array_values($params);

            return new $class(...$params);
        }

        return $object;
    }
}
