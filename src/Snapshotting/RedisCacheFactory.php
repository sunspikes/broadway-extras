<?php

namespace Sunspikes\Broadway\Snapshotting;

use Doctrine\Common\Cache\RedisCache;
use Redis;

class RedisCacheFactory
{
    /**
     * @param string $host
     * @param string $port
     * @return RedisCache
     * @throws \Exception
     */
    public static function createCache(string $host, string $port): RedisCache
    {
        $redis = new Redis();

        if (false === $redis->pconnect($host, $port)) {
            throw new \Exception('Cannot connect to redis.');
        }

        $redis->setOption(Redis::OPT_SERIALIZER, Redis::SERIALIZER_IGBINARY);

        $redisCache = new RedisCache();
        $redisCache->setRedis($redis);

        return $redisCache;
    }
}
