<?php

declare(strict_types=1);

namespace Snapflow\Database;

use Exception;
use InvalidArgumentException;
use PDO;
use PDOException;
use PDOStatement;
use Snapflow\Database\Adapters\AdapterFactory;
use Snapflow\Database\Adapters\DatabaseAdapterInterface;
use Snapflow\Database\Builders\QueryBuilder;
use Snapflow\Database\Mappers\DataMapper;
use Snapflow\Database\Raw;

class Database
{
    public ?PDO $pdo = null;
    public string $type;
    public string $queryString = '';
    public string $returnId = '';
    public ?string $error = null;
    public ?array $errorInfo = null;

    protected string $prefix = '';
    protected ?PDOStatement $statement = null;
    protected string $dsn = '';
    protected array $logs = [];
    protected bool $logging = false;
    protected bool $testMode = false;
    protected bool $debugMode = false;
    protected bool $debugLogging = false;
    protected array $debugLogs = [];

    private DatabaseAdapterInterface $adapter;
    private QueryBuilder $queryBuilder;

    public function __construct(array $options)
    {
        $this->prefix = $options['prefix'] ?? '';

        if (isset($options['testMode']) && $options['testMode'] === true) {
            $this->testMode = true;
            $this->type = $options['type'] ?? 'mysql';
            $this->adapter = AdapterFactory::create($this->type);
            $this->initializeQueryBuilder();
            return;
        }

        $options['type'] = $options['type'] ?? $options['database_type'] ?? null;

        if (!$options['type']) {
            throw new InvalidArgumentException('Database type is required.');
        }

        $this->adapter = AdapterFactory::create($options['type']);
        $this->type = $this->adapter->getType();

        if (!isset($options['pdo'])) {
            $this->validateConnectionOptions($options);
        }

        $this->logging = $options['logging'] ?? false;

        if (isset($options['pdo'])) {
            $this->initializeWithPdo($options['pdo'], $options);
            return;
        }

        $this->initializeWithOptions($options);
    }

    private function validateConnectionOptions(array $options): void
    {
        $options['database'] = $options['database'] ?? $options['database_name'] ?? null;

        if (!isset($options['socket']) && !isset($options['dsn'])) {
            $options['host'] = $options['host'] ?? $options['server'] ?? false;
        }
    }

    private function initializeWithPdo(PDO $pdo, array $options): void
    {
        if (!$pdo instanceof PDO) {
            throw new InvalidArgumentException('Invalid PDO object supplied.');
        }

        $this->pdo = $pdo;
        $this->initializeQueryBuilder();

        foreach ($this->adapter->getInitCommands($options) as $command) {
            $this->pdo->exec($command);
        }
    }

    private function initializeWithOptions(array $options): void
    {
        $dsn = isset($options['dsn']) ? $this->buildDsnFromArray($options['dsn']) : $this->adapter->buildDsn($options);

        $driver = explode(':', $dsn)[0];

        if (!in_array($driver, PDO::getAvailableDrivers())) {
            throw new InvalidArgumentException("Unsupported PDO driver: {$driver}.");
        }

        $this->dsn = $dsn;

        try {
            $this->pdo = new PDO(
                $dsn,
                $options['username'] ?? null,
                $options['password'] ?? null,
                $options['option'] ?? []
            );

            if (isset($options['error'])) {
                $errorMode = in_array($options['error'], [PDO::ERRMODE_SILENT, PDO::ERRMODE_WARNING, PDO::ERRMODE_EXCEPTION])
                    ? $options['error']
                    : PDO::ERRMODE_SILENT;
                $this->pdo->setAttribute(PDO::ATTR_ERRMODE, $errorMode);
            }

            $commands = $this->adapter->getInitCommands($options);

            if (isset($options['command']) && is_array($options['command'])) {
                $commands = array_merge($commands, $options['command']);
            }

            foreach ($commands as $command) {
                $this->pdo->exec($command);
            }

            $this->initializeQueryBuilder();
        } catch (PDOException $e) {
            throw new PDOException($e->getMessage());
        }
    }

    private function buildDsnFromArray(array $dsnArray): string
    {
        if (!isset($dsnArray['driver'])) {
            throw new InvalidArgumentException('Invalid DSN option supplied.');
        }

        return $dsnArray['driver'];
    }

    private function initializeQueryBuilder(): void
    {
        $this->queryBuilder = new QueryBuilder(
            $this->prefix,
            $this->adapter->getQuotePattern(),
            $this->adapter->getTableAliasConnector(),
            $this->type
        );
    }

    public static function raw(string $string, array $map = []): Raw
    {
        $raw = new Raw($string, $map);
        return $raw;
    }

    public function quote(string $string): string
    {
        return $this->queryBuilder->quote($string);
    }

    public function tableQuote(string $table): string
    {
        return $this->queryBuilder->tableQuote($table);
    }

    public function columnQuote(string $column): string
    {
        return $this->queryBuilder->columnQuote($column);
    }

    public function query(string $statement, array $map = []): ?PDOStatement
    {
        $raw = self::raw($statement, $map);
        $statement = $this->queryBuilder->buildRaw($raw, $map);

        return $this->exec($statement, $map);
    }

    public function exec(string $statement, array $map = [], ?callable $callback = null): ?PDOStatement
    {
        $this->statement = null;
        $this->errorInfo = null;
        $this->error = null;

        if ($this->testMode) {
            $this->queryString = $this->generate($statement, $map);
            return null;
        }

        if ($this->debugMode) {
            if ($this->debugLogging) {
                $this->debugLogs[] = $this->generate($statement, $map);
                return null;
            }

            echo $this->generate($statement, $map);
            $this->debugMode = false;
            return null;
        }

        if ($this->logging) {
            $this->logs[] = [$statement, $map];
        } else {
            $this->logs = [[$statement, $map]];
        }

        $statement = $this->pdo->prepare($statement);
        $errorInfo = $this->pdo->errorInfo();

        if ($errorInfo[0] !== '00000') {
            $this->errorInfo = $errorInfo;
            $this->error = $errorInfo[2];
            return null;
        }

        foreach ($map as $key => $value) {
            $statement->bindValue($key, $value[0], $value[1]);
        }

        if (is_callable($callback)) {
            $this->pdo->beginTransaction();
            $callback($statement);
            $execute = $statement->execute();
            $this->pdo->commit();
        } else {
            $execute = $statement->execute();
        }

        $errorInfo = $statement->errorInfo();

        if ($errorInfo[0] !== '00000') {
            $this->errorInfo = $errorInfo;
            $this->error = $errorInfo[2];
            return null;
        }

        if ($execute) {
            $this->statement = $statement;
        }

        return $statement;
    }

    protected function generate(string $statement, array $map): string
    {
        $statement = preg_replace(
            '/(?!\'[^\s]+\s?)"([\p{L}_][\p{L}\p{N}@$#\-_\.]*)"(?!\s?[^\s]+\')/u',
            $this->adapter->getQuotePattern(),
            $statement
        );

        foreach ($map as $key => $value) {
            $replace = match ($value[1]) {
                PDO::PARAM_STR => $this->quote((string)$value[0]),
                PDO::PARAM_NULL => 'NULL',
                PDO::PARAM_LOB => '{LOB_DATA}',
                default => $value[0] . ''
            };

            $statement = str_replace($key, $replace, $statement);
        }

        return $statement;
    }

    public function create(string $table, $columns, $options = null): ?PDOStatement
    {
        $stack = [];
        $tableName = $this->queryBuilder->tableQuote($table);

        foreach ($columns as $name => $definition) {
            if (is_int($name)) {
                $stack[] = preg_replace("/<([\p{L}_][\p{L}\p{N}@$#\-_\.]*)>/u", '"$1"', $definition);
            } elseif (is_array($definition)) {
                $stack[] = $this->queryBuilder->columnQuote($name) . ' ' . implode(' ', $definition);
            } elseif (is_string($definition)) {
                $stack[] = $this->queryBuilder->columnQuote($name) . ' ' . $definition;
            }
        }

        $tableOption = '';

        if (is_array($options)) {
            $optionStack = [];
            foreach ($options as $key => $value) {
                if (is_string($value) || is_int($value)) {
                    $optionStack[] = "{$key} = {$value}";
                }
            }
            $tableOption = ' ' . implode(', ', $optionStack);
        } elseif (is_string($options)) {
            $tableOption = ' ' . $options;
        }

        $command = 'CREATE TABLE';

        if (in_array($this->type, ['mysql', 'pgsql', 'sqlite'])) {
            $command .= ' IF NOT EXISTS';
        }

        return $this->exec("{$command} {$tableName} (" . implode(', ', $stack) . "){$tableOption}");
    }

    public function drop(string $table): ?PDOStatement
    {
        return $this->exec('DROP TABLE IF EXISTS ' . $this->queryBuilder->tableQuote($table));
    }

    public function select(string $table, $join, $columns = null, $where = null): ?array
    {
        $map = [];
        $result = [];
        $columnMap = [];

        $args = func_get_args();
        $lastArgs = $args[array_key_last($args)];
        $callback = is_callable($lastArgs) ? $lastArgs : null;

        $where = is_callable($where) ? null : $where;
        $columns = is_callable($columns) ? null : $columns;

        $column = $where === null ? $join : $columns;
        $isSingle = (is_string($column) && $column !== '*');

        $statement = $this->exec($this->queryBuilder->buildSelect($table, $map, $join, $columns, $where), $map);

        $this->queryBuilder->columnMap($columns, $columnMap, true);

        if (!$this->statement) {
            return $result;
        }

        if ($columns === '*') {
            if (isset($callback)) {
                while ($data = $statement->fetch(PDO::FETCH_ASSOC)) {
                    $callback($data);
                }
                return null;
            }

            return $statement->fetchAll(PDO::FETCH_ASSOC);
        }

        $dataMapper = new DataMapper(isset($callback));

        while ($data = $statement->fetch(PDO::FETCH_ASSOC)) {
            $currentStack = [];

            if (isset($callback)) {
                $dataMapper->map($data, $columns, $columnMap, $currentStack, true);
                $callback($isSingle ? $currentStack[$columnMap[$column][0]] : $currentStack);
            } else {
                $dataMapper->map($data, $columns, $columnMap, $currentStack, true, $result);
            }
        }

        if (isset($callback)) {
            return null;
        }

        if ($isSingle) {
            $singleResult = [];
            $resultKey = $columnMap[$column][0];

            foreach ($result as $item) {
                $singleResult[] = $item[$resultKey];
            }

            return $singleResult;
        }

        return $result;
    }

    public function insert(string $table, array $values, ?string $primaryKey = null): ?PDOStatement
    {
        $stack = [];
        $columns = [];
        $fields = [];
        $map = [];
        $returnings = [];

        if (!isset($values[0])) {
            $values = [$values];
        }

        foreach ($values as $data) {
            foreach ($data as $key => $value) {
                $columns[] = $key;
            }
        }

        $columns = array_unique($columns);

        foreach ($values as $data) {
            $values = [];

            foreach ($columns as $key) {
                $value = $data[$key];
                $type = gettype($value);

                if ($this->type === 'oracle' && $type === 'resource') {
                    $values[] = 'EMPTY_BLOB()';
                    $returnings[$this->queryBuilder->mapKey()] = [$key, $value, PDO::PARAM_LOB];
                    continue;
                }

                if ($raw = $this->queryBuilder->buildRaw($data[$key], $map)) {
                    $values[] = $raw;
                    continue;
                }

                $mapKey = $this->queryBuilder->mapKey();
                $values[] = $mapKey;

                $map = $this->getArr($type, $key, $value, $map, $mapKey);
            }

            $stack[] = '(' . implode(', ', $values) . ')';
        }

        foreach ($columns as $key) {
            $fields[] = $this->queryBuilder->columnQuote(preg_replace("/(\s*\[JSON]$)/i", '', $key));
        }

        $query = 'INSERT INTO ' . $this->queryBuilder->tableQuote($table) . ' (' . implode(', ', $fields) . ') VALUES ' . implode(', ', $stack);

        if ($this->type === 'oracle' && (!empty($returnings) || isset($primaryKey))) {
            if ($primaryKey) {
                $returnings[':RETURNID'] = [$primaryKey, '', PDO::PARAM_INT, 8];
            }

            $statement = $this->returningQuery($query, $map, $returnings);

            if ($primaryKey) {
                $this->returnId = $returnings[':RETURNID'][1];
            }

            return $statement;
        }

        return $this->exec($query, $map);
    }

    public function update(string $table, $data, $where = null): ?PDOStatement
    {
        $fields = [];
        $map = [];
        $returnings = [];

        foreach ($data as $key => $value) {
            $column = $this->queryBuilder->columnQuote(preg_replace("/(\s*\[(JSON|\+|\-|\*|\/)\]$)/", '', $key));
            $type = gettype($value);

            if ($this->type === 'oracle' && $type === 'resource') {
                $fields[] = "{$column} = EMPTY_BLOB()";
                $returnings[$this->queryBuilder->mapKey()] = [$key, $value, PDO::PARAM_LOB];
                continue;
            }

            if ($raw = $this->queryBuilder->buildRaw($value, $map)) {
                $fields[] = "{$column} = {$raw}";
                continue;
            }

            preg_match("/([\p{L}_][\p{L}\p{N}@$#\-_\.]*)(\[(?<operator>\+|\-|\*|\/)\])?/u", $key, $match);

            if (isset($match['operator'])) {
                if (is_numeric($value)) {
                    $fields[] = "{$column} = {$column} {$match['operator']} {$value}";
                }
            } else {
                $mapKey = $this->queryBuilder->mapKey();
                $fields[] = "{$column} = {$mapKey}";

                $map = $this->getArr($type, $key, $value, $map, $mapKey);
            }
        }

        $query = 'UPDATE ' . $this->queryBuilder->tableQuote($table) . ' SET ' . implode(', ', $fields) . $this->queryBuilder->whereClause($where, $map);

        if ($this->type === 'oracle' && !empty($returnings)) {
            return $this->returningQuery($query, $map, $returnings);
        }

        return $this->exec($query, $map);
    }

    public function delete(string $table, $where): ?PDOStatement
    {
        $map = [];
        return $this->exec('DELETE FROM ' . $this->queryBuilder->tableQuote($table) . $this->queryBuilder->whereClause($where, $map), $map);
    }

    public function replace(string $table, array $columns, $where = null): ?PDOStatement
    {
        $map = [];
        $stack = [];

        foreach ($columns as $column => $replacements) {
            if (is_array($replacements)) {
                foreach ($replacements as $old => $new) {
                    $mapKey = $this->queryBuilder->mapKey();
                    $columnName = $this->queryBuilder->columnQuote($column);
                    $stack[] = "{$columnName} = REPLACE({$columnName}, {$mapKey}a, {$mapKey}b)";

                    $map[$mapKey . 'a'] = [$old, PDO::PARAM_STR];
                    $map[$mapKey . 'b'] = [$new, PDO::PARAM_STR];
                }
            }
        }

        if (empty($stack)) {
            throw new InvalidArgumentException('Invalid columns supplied.');
        }

        return $this->exec('UPDATE ' . $this->queryBuilder->tableQuote($table) . ' SET ' . implode(', ', $stack) . $this->queryBuilder->whereClause($where, $map), $map);
    }

    public function get(string $table, $join = null, $columns = null, $where = null)
    {
        $map = [];
        $result = [];
        $columnMap = [];
        $currentStack = [];

        if ($where === null) {
            if ($this->isJoin($join)) {
                $where['LIMIT'] = 1;
            } else {
                $columns['LIMIT'] = 1;
            }
            $column = $join;
        } else {
            $column = $columns;
            $where['LIMIT'] = 1;
        }

        $isSingle = (is_string($column) && $column !== '*');
        $query = $this->exec($this->queryBuilder->buildSelect($table, $map, $join, $columns, $where), $map);

        if (!$this->statement) {
            return false;
        }

        $data = $query->fetchAll(PDO::FETCH_ASSOC);

        if (isset($data[0])) {
            if ($column === '*') {
                return $data[0];
            }

            $this->queryBuilder->columnMap($columns, $columnMap, true);
            $dataMapper = new DataMapper(false);
            $dataMapper->map($data[0], $columns, $columnMap, $currentStack, true, $result);

            if ($isSingle) {
                return $result[0][$columnMap[$column][0]];
            }

            return $result[0];
        }

        return null;
    }

    private function isJoin($join): bool
    {
        if (!is_array($join)) {
            return false;
        }

        $keys = array_keys($join);
        return isset($keys[0]) && is_string($keys[0]) && strpos($keys[0], '[') === 0;
    }

    public function has(string $table, $join, $where = null): bool
    {
        $map = [];
        $column = null;

        $query = $this->exec(
            $this->type === 'mssql'
                ? $this->queryBuilder->buildSelect($table, $map, $join, $column, $where, self::raw('TOP 1 1'))
                : 'SELECT EXISTS(' . $this->queryBuilder->buildSelect($table, $map, $join, $column, $where, 1) . ')',
            $map
        );

        if (!$this->statement) {
            return false;
        }

        $result = $query->fetchColumn();
        return $result === '1' || $result === 1 || $result === true;
    }

    public function rand(string $table, $join = null, $columns = null, $where = null): array
    {
        $orderRaw = self::raw(
            $this->type === 'mysql' ? 'RAND()' : ($this->type === 'mssql' ? 'NEWID()' : 'RANDOM()')
        );

        if ($where === null) {
            if ($this->isJoin($join)) {
                $where['ORDER'] = $orderRaw;
            } else {
                $columns['ORDER'] = $orderRaw;
            }
        } else {
            $where['ORDER'] = $orderRaw;
        }

        return $this->select($table, $join, $columns, $where);
    }

    private function aggregate(string $type, string $table, $join = null, $column = null, $where = null): ?string
    {
        $map = [];
        $query = $this->exec($this->queryBuilder->buildSelect($table, $map, $join, $column, $where, $type), $map);

        if (!$this->statement) {
            return null;
        }

        return (string)$query->fetchColumn();
    }

    public function count(string $table, $join = null, $column = null, $where = null): ?int
    {
        return (int)$this->aggregate('COUNT', $table, $join, $column, $where);
    }

    public function avg(string $table, $join, $column = null, $where = null): ?string
    {
        return $this->aggregate('AVG', $table, $join, $column, $where);
    }

    public function max(string $table, $join, $column = null, $where = null): ?string
    {
        return $this->aggregate('MAX', $table, $join, $column, $where);
    }

    public function min(string $table, $join, $column = null, $where = null): ?string
    {
        return $this->aggregate('MIN', $table, $join, $column, $where);
    }

    public function sum(string $table, $join, $column = null, $where = null): ?string
    {
        return $this->aggregate('SUM', $table, $join, $column, $where);
    }

    public function action(callable $actions): void
    {
        if (is_callable($actions)) {
            $this->pdo->beginTransaction();

            try {
                $result = $actions($this);

                if ($result === false) {
                    $this->pdo->rollBack();
                } else {
                    $this->pdo->commit();
                }
            } catch (Exception $e) {
                $this->pdo->rollBack();
                throw $e;
            }
        }
    }

    public function id(?string $name = null): ?string
    {
        if ($this->type === 'oracle') {
            return $this->returnId;
        }

        if ($this->type === 'pgsql') {
            $id = $this->pdo->query('SELECT LASTVAL()')->fetchColumn();
            return (string)$id ?: null;
        }

        return $this->pdo->lastInsertId($name);
    }

    public function debug(): self
    {
        $this->debugMode = true;
        return $this;
    }

    public function beginDebug(): void
    {
        $this->debugMode = true;
        $this->debugLogging = true;
    }

    public function debugLog(): array
    {
        $this->debugMode = false;
        $this->debugLogging = false;
        return $this->debugLogs;
    }

    public function last(): ?string
    {
        if (empty($this->logs)) {
            return null;
        }

        $log = $this->logs[array_key_last($this->logs)];
        return $this->generate($log[0], $log[1]);
    }

    public function log(): array
    {
        return array_map(
            fn($log) => $this->generate($log[0], $log[1]),
            $this->logs
        );
    }

    public function info(): array
    {
        $output = [
            'server' => 'SERVER_INFO',
            'driver' => 'DRIVER_NAME',
            'client' => 'CLIENT_VERSION',
            'version' => 'SERVER_VERSION',
            'connection' => 'CONNECTION_STATUS'
        ];

        foreach ($output as $key => $value) {
            try {
                $output[$key] = $this->pdo->getAttribute(constant('PDO::ATTR_' . $value));
            } catch (PDOException $e) {
                $output[$key] = $e->getMessage();
            }
        }

        $output['dsn'] = $this->dsn;

        return $output;
    }

    private function returningQuery(string $query, array &$map, array &$data): ?PDOStatement
    {
        $returnColumns = array_map(fn($value) => $value[0], $data);

        $query .= ' RETURNING ' .
            implode(', ', array_map([$this->queryBuilder, 'columnQuote'], $returnColumns)) .
            ' INTO ' .
            implode(', ', array_keys($data));

        return $this->exec($query, $map, function ($statement) use (&$data) {
            foreach ($data as $key => $return) {
                if (isset($return[3])) {
                    $statement->bindParam($key, $data[$key][1], $return[2], $return[3]);
                } else {
                    $statement->bindParam($key, $data[$key][1], $return[2]);
                }
            }
        });
    }

    /**
     * @param string $type
     * @param int|string $key
     * @param mixed $value
     * @param array $map
     * @param string $mapKey
     * @return array
     */
    public function getArr(string $type, int|string $key, mixed $value, array $map, string $mapKey): array
    {
        if ($type === 'array') {
            $map[$mapKey] = [
                strpos($key, '[JSON]') === strlen($key) - 6 ? json_encode($value) : serialize($value),
                PDO::PARAM_STR
            ];
        } elseif ($type === 'object') {
            $map[$mapKey] = [serialize($value), PDO::PARAM_STR];
        } else {
            $map[$mapKey] = $this->queryBuilder->typeMap($value, $type);
        }
        return $map;
    }
}
