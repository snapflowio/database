<?php

declare(strict_types=1);

namespace Snapflow\Database\Builders;

class JoinBuilder
{
    private const TABLE_PATTERN = "[\p{L}_][\p{L}\p{N}@$#\-_]*";
    private const ALIAS_PATTERN = "[\p{L}_][\p{L}\p{N}@$#\-_]*";

    private array $type = [
        '>' => 'LEFT',
        '<' => 'RIGHT',
        '<>' => 'FULL',
        '><' => 'INNER'
    ];

    public function __construct(
        private QueryBuilder $queryBuilder
    )
    {
    }

    public function isJoin($join): bool
    {
        if (!is_array($join)) {
            return false;
        }

        $keys = array_keys($join);

        return isset($keys[0]) && is_string($keys[0]) && strpos($keys[0], '[') === 0;
    }

    public function build(string $table, array $join, array &$map): string
    {
        $tableJoin = [];

        foreach ($join as $subtable => $relation) {
            preg_match("/(\[(?<join>\<\>?|\>\<?)\])?(?<table>" . self::TABLE_PATTERN . ")\s?(\((?<alias>" . self::ALIAS_PATTERN . ")\))?/u", $subtable, $match);

            if ($match['join'] === '' || $match['table'] === '') {
                continue;
            }

            if (is_string($relation)) {
                $relation = 'USING ("' . $relation . '")';
            } elseif (is_array($relation)) {
                if (isset($relation[0])) {
                    $relation = 'USING ("' . implode('", "', $relation) . '")';
                } else {
                    $joins = [];

                    foreach ($relation as $key => $value) {
                        if ($key === 'AND' && is_array($value)) {
                            $whereBuilder = new WhereBuilder($this->queryBuilder);
                            $joins[] = $whereBuilder->implode($value, $map, ' AND');
                            continue;
                        }

                        $joins[] = (
                            strpos($key, '.') > 0
                                ? $this->queryBuilder->columnQuote($key)
                                : $table . '.' . $this->queryBuilder->columnQuote($key)
                            ) . ' = ' . $this->queryBuilder->tableQuote($match['alias'] ?? $match['table']) . '.' . $this->queryBuilder->columnQuote($value);
                    }

                    $relation = 'ON ' . implode(' AND ', $joins);
                }
            } elseif ($raw = $this->queryBuilder->buildRaw($relation, $map)) {
                $relation = $raw;
            }

            $tableName = $this->queryBuilder->tableQuote($match['table']);
            if (isset($match['alias'])) {
                $tableName .= $this->queryBuilder->getTableAliasConnector() . $this->queryBuilder->tableQuote($match['alias']);
            }

            $tableJoin[] = $this->type[$match['join']] . " JOIN {$tableName} {$relation}";
        }

        return implode(' ', $tableJoin);
    }
}
