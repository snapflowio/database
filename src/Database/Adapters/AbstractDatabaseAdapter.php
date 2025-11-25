<?php

declare(strict_types=1);

namespace Snapflow\Database\Adapters;

abstract class AbstractDatabaseAdapter implements DatabaseAdapterInterface
{
    protected string $type;
    protected string $quotePattern = '"$1"';
    protected string $tableAliasConnector = ' AS ';

    public function getType(): string
    {
        return $this->type;
    }

    public function getQuotePattern(): string
    {
        return $this->quotePattern;
    }

    public function getTableAliasConnector(): string
    {
        return $this->tableAliasConnector;
    }

    public function getInitCommands(array $options): array
    {
        return [];
    }

    protected function buildBasicDsn(string $driver, array $attributes): string
    {
        unset($attributes['driver']);

        $stack = [];
        foreach ($attributes as $key => $value) {
            $stack[] = is_int($key) ? $value : $key . '=' . $value;
        }
        return $driver . ':' . implode(';', $stack);
    }
}
