<?php

declare(strict_types=1);

namespace Snapflow\Database\Builders;

use InvalidArgumentException;
use Snapflow\Database\Raw;
use PDO;

class QueryBuilder
{
    private const TABLE_PATTERN = "[\p{L}_][\p{L}\p{N}@$#\-_]*";
    private const COLUMN_PATTERN = "[\p{L}_][\p{L}\p{N}@$#\-_\.]*";

    private int $guid = 0;
    private ColumnBuilder $columnBuilder;
    private JoinBuilder $joinBuilder;
    private WhereBuilder $whereBuilder;

    public function __construct(
        private string $prefix,
        private string $quotePattern,
        private string $tableAliasConnector,
        private string $type
    )
    {
        $this->columnBuilder = new ColumnBuilder($this);
        $this->joinBuilder = new JoinBuilder($this);
        $this->whereBuilder = new WhereBuilder($this);
    }

    public function mapKey(): string
    {
        return ':MeD' . $this->guid++ . '_mK';
    }

    public function quote(string $string): string
    {
        if ($this->type === 'mysql') {
            return "'" . preg_replace(['/([\'"])/', '/(\\\\\\\")/'], ["\\\\\${1}", '\\\${1}'], $string) . "'";
        }

        return "'" . preg_replace('/\'/', '\'\'', $string) . "'";
    }

    public function tableQuote(string $table): string
    {
        if (!preg_match("/^" . self::TABLE_PATTERN . "$/u", $table)) {
            throw new InvalidArgumentException("Incorrect table name: {$table}.");
        }

        return '"' . $this->prefix . $table . '"';
    }

    public function columnQuote(string $column): string
    {
        if (!preg_match("/^" . self::TABLE_PATTERN . "(\.?" . self::TABLE_PATTERN . ")?$/u", $column)) {
            throw new InvalidArgumentException("Incorrect column name: {$column}.");
        }

        return strpos($column, '.') !== false
            ? '"' . $this->prefix . str_replace('.', '"."', $column) . '"'
            : '"' . $column . '"';
    }

    public function typeMap($value, string $type): array
    {
        $map = [
            'NULL' => PDO::PARAM_NULL,
            'integer' => PDO::PARAM_INT,
            'double' => PDO::PARAM_STR,
            'boolean' => PDO::PARAM_BOOL,
            'string' => PDO::PARAM_STR,
            'object' => PDO::PARAM_STR,
            'resource' => PDO::PARAM_LOB
        ];

        if ($type === 'boolean') {
            $value = $value ? '1' : '0';
        } elseif ($type === 'NULL') {
            $value = null;
        }

        return [$value, $map[$type]];
    }

    public function isRaw($object): bool
    {
        return $object instanceof Raw;
    }

    public function buildRaw($raw, array &$map): ?string
    {
        if (!$this->isRaw($raw)) {
            return null;
        }

        $query = preg_replace_callback(
            '/(([`\'])[\<]*?)?((FROM|TABLE|TABLES LIKE|INTO|UPDATE|JOIN|TABLE IF EXISTS)\s*)?\<((' . self::TABLE_PATTERN . ')(\.' . self::COLUMN_PATTERN . ')?)\>([^,]*?\2)?/',
            function ($matches) {
                if (!empty($matches[2]) && isset($matches[8])) {
                    return $matches[0];
                }

                if (!empty($matches[4])) {
                    return $matches[1] . $matches[4] . ' ' . $this->tableQuote($matches[5]);
                }

                return $matches[1] . $this->columnQuote($matches[5]);
            },
            $raw->value
        );

        $rawMap = $raw->map;

        if (!empty($rawMap)) {
            foreach ($rawMap as $key => $value) {
                $map[$key] = $this->typeMap($value, gettype($value));
            }
        }

        return $query;
    }

    public function getTableAliasConnector(): string
    {
        return $this->tableAliasConnector;
    }

    public function buildSelect(
        string $table,
        array  &$map,
               $join,
               &$columns = null,
               $where = null,
               $columnFn = null
    ): string
    {
        preg_match("/(?<table>" . self::TABLE_PATTERN . ")\s*\((?<alias>" . self::TABLE_PATTERN . ")\)/u", $table, $tableMatch);

        if (isset($tableMatch['table'], $tableMatch['alias'])) {
            $table = $this->tableQuote($tableMatch['table']);
            $tableAlias = $this->tableQuote($tableMatch['alias']);
            $tableQuery = "{$table}{$this->tableAliasConnector}{$tableAlias}";
        } else {
            $table = $this->tableQuote($table);
            $tableQuery = $table;
        }

        $isJoin = $this->joinBuilder->isJoin($join);

        if ($isJoin) {
            $tableQuery .= ' ' . $this->joinBuilder->build($tableAlias ?? $table, $join, $map);
        } else {
            if (is_null($columns)) {
                if (!is_null($where) || (is_array($join) && isset($columnFn))) {
                    $where = $join;
                    $columns = null;
                } else {
                    $where = null;
                    $columns = $join;
                }
            } else {
                $where = $columns;
                $columns = $join;
            }
        }

        if (isset($columnFn)) {
            if ($columnFn === 1) {
                $column = '1';
                if (is_null($where)) {
                    $where = $columns;
                }
            } elseif ($raw = $this->buildRaw($columnFn, $map)) {
                $column = $raw;
            } else {
                if (empty($columns) || $this->isRaw($columns)) {
                    $columns = '*';
                    $where = $join;
                }

                $column = $columnFn . '(' . $this->columnBuilder->push($columns, $map, true) . ')';
            }
        } else {
            $column = $this->columnBuilder->push($columns, $map, true, $isJoin);
        }

        return 'SELECT ' . $column . ' FROM ' . $tableQuery . $this->whereBuilder->buildClause($where, $map, $this->type);
    }

    public function columnPush(&$columns, array &$map, bool $root, bool $isJoin = false): string
    {
        return $this->columnBuilder->push($columns, $map, $root, $isJoin);
    }

    public function columnMap($columns, array &$stack, bool $root): array
    {
        return $this->columnBuilder->map($columns, $stack, $root);
    }

    public function whereClause($where, array &$map): string
    {
        return $this->whereBuilder->buildClause($where, $map, $this->type);
    }
}
