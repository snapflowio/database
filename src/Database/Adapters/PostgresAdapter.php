<?php

declare(strict_types=1);

namespace Snapflow\Database\Adapters;

class PostgresAdapter extends AbstractDatabaseAdapter
{
    protected string $type = 'pgsql';

    public function buildDsn(array $options): string
    {
        $attr = [
            'driver' => 'pgsql',
            'host' => $options['host'],
            'dbname' => $options['database']
        ];

        if (isset($options['port']) && is_numeric($options['port'])) {
            $attr['port'] = $options['port'];
        }

        return $this->buildBasicDsn('pgsql', $attr);
    }

    public function getInitCommands(array $options): array
    {
        if (isset($options['charset'])) {
            return ["SET NAMES '{$options['charset']}'"];
        }

        return [];
    }
}
