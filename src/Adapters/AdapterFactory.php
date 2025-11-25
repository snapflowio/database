<?php

declare(strict_types=1);

namespace Snapflow\Database\Adapters;

use InvalidArgumentException;

class AdapterFactory
{
    private static array $adapters = [
        'postgres' => PostgresAdapter::class,
        'postgresql' => PostgresAdapter::class,
        'pgsql' => PostgresAdapter::class,
    ];

    public static function create(string $type): DatabaseAdapterInterface
    {
        $type = strtolower($type);

        if (!isset(self::$adapters[$type])) {
            throw new InvalidArgumentException(
                "Unsupported database type: {$type}. Only PostgreSQL is supported."
            );
        }

        $adapterClass = self::$adapters[$type];
        return new $adapterClass();
    }
}
