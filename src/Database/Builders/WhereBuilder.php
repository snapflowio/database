<?php

declare(strict_types=1);

namespace Snapflow\Database\Builders;

use InvalidArgumentException;
use PDO;

class WhereBuilder
{
    private const COLUMN_PATTERN = "[\p{L}_][\p{L}\p{N}@$#\-_\.]*";

    public function __construct(
        private QueryBuilder $queryBuilder
    )
    {
    }

    public function implode(array $data, array &$map, string $conjunctor): string
    {
        $stack = [];

        foreach ($data as $key => $value) {
            $type = gettype($value);

            if ($type === 'array' && preg_match("/^(AND|OR)(\s+#.*)?$/", $key, $relationMatch)) {
                $stack[] = '(' . $this->implode($value, $map, ' ' . $relationMatch[1]) . ')';
                continue;
            }

            $mapKey = $this->queryBuilder->mapKey();
            $isIndex = is_int($key);

            preg_match(
                "/(?<column>" . self::COLUMN_PATTERN . ")(\[(?<operator>.*)])?(?<comparison>" . self::COLUMN_PATTERN . ")?/u",
                $isIndex ? $value : $key,
                $match
            );

            $column = $this->queryBuilder->columnQuote($match['column']);
            $operator = $match['operator'] ?? null;

            if ($isIndex && isset($match['comparison']) && in_array($operator, ['>', '>=', '<', '<=', '=', '!='])) {
                $stack[] = "{$column} {$operator} " . $this->queryBuilder->columnQuote($match['comparison']);
                continue;
            }

            if ($operator && $operator !== '=') {
                $stack[] = $this->handleOperator($operator, $column, $value, $type, $mapKey, $map);
                continue;
            }

            $stack[] = $this->handleDefaultOperator($column, $value, $type, $mapKey, $map);
        }

        return implode($conjunctor . ' ', $stack);
    }

    private function handleOperator(string $operator, string $column, $value, string $type, string $mapKey, array &$map): string
    {
        if (in_array($operator, ['>', '>=', '<', '<='])) {
            return $this->handleComparisonOperator($operator, $column, $value, $mapKey, $map);
        }

        if ($operator === '!') {
            return $this->handleNotOperator($column, $value, $type, $mapKey, $map);
        }

        if ($operator === '~' || $operator === '!~') {
            return $this->handleLikeOperator($operator, $column, $value, $type, $mapKey, $map);
        }

        if ($operator === '<>' || $operator === '><') {
            return $this->handleBetweenOperator($operator, $column, $value, $mapKey, $map);
        }

        if ($operator === 'REGEXP') {
            $map[$mapKey] = [$value, PDO::PARAM_STR];
            return "{$column} REGEXP {$mapKey}";
        }

        throw new InvalidArgumentException("Invalid operator [{$operator}] for column {$column} supplied.");
    }

    private function handleComparisonOperator(string $operator, string $column, $value, string $mapKey, array &$map): string
    {
        $condition = "{$column} {$operator} ";

        if (is_numeric($value)) {
            $condition .= $mapKey;
            $map[$mapKey] = [$value, is_float($value) ? PDO::PARAM_STR : PDO::PARAM_INT];
        } elseif ($raw = $this->queryBuilder->buildRaw($value, $map)) {
            $condition .= $raw;
        } else {
            $condition .= $mapKey;
            $map[$mapKey] = [$value, PDO::PARAM_STR];
        }

        return $condition;
    }

    private function handleNotOperator(string $column, $value, string $type, string $mapKey, array &$map): string
    {
        switch ($type) {
            case 'NULL':
                return $column . ' IS NOT NULL';

            case 'array':
                $values = [];
                foreach ($value as $index => $item) {
                    if ($raw = $this->queryBuilder->buildRaw($item, $map)) {
                        $values[] = $raw;
                    } else {
                        $stackKey = $mapKey . $index . '_i';
                        $values[] = $stackKey;
                        $map[$stackKey] = $this->queryBuilder->typeMap($item, gettype($item));
                    }
                }
                return $column . ' NOT IN (' . implode(', ', $values) . ')';

            case 'object':
                if ($raw = $this->queryBuilder->buildRaw($value, $map)) {
                    return "{$column} != {$raw}";
                }
                break;

            case 'integer':
            case 'double':
            case 'boolean':
            case 'string':
                $map[$mapKey] = $this->queryBuilder->typeMap($value, $type);
                return "{$column} != {$mapKey}";
        }

        return '';
    }

    private function handleLikeOperator(string $operator, string $column, $value, string $type, string $mapKey, array &$map): string
    {
        if ($type !== 'array') {
            $value = [$value];
        }

        $connector = ' OR ';
        $data = array_values($value);

        if (is_array($data[0])) {
            if (isset($value['AND']) || isset($value['OR'])) {
                $connector = ' ' . array_keys($value)[0] . ' ';
                $value = $data[0];
            }
        }

        $likeClauses = [];

        foreach ($value as $index => $item) {
            $likeKey = "{$mapKey}_{$index}_i";
            $item = strval($item);

            if (!preg_match('/((?<!\\\)\[.+(?<!\\\)\]|(?<!\\\)[\*\?\!\%#^_]|%.+|.+%)/', $item)) {
                $item = '%' . $item . '%';
            }

            $likeClauses[] = $column . ($operator === '!~' ? ' NOT' : '') . " LIKE {$likeKey}";
            $map[$likeKey] = [$item, PDO::PARAM_STR];
        }

        return '(' . implode($connector, $likeClauses) . ')';
    }

    private function handleBetweenOperator(string $operator, string $column, $value, string $mapKey, array &$map): string
    {
        if (!is_array($value)) {
            return '';
        }

        if ($operator === '><') {
            $column .= ' NOT';
        }

        if ($this->queryBuilder->isRaw($value[0]) && $this->queryBuilder->isRaw($value[1])) {
            return "({$column} BETWEEN {$this->queryBuilder->buildRaw($value[0], $map)} AND {$this->queryBuilder->buildRaw($value[1], $map)})";
        }

        $dataType = (is_numeric($value[0]) && is_numeric($value[1])) ? PDO::PARAM_INT : PDO::PARAM_STR;
        $map[$mapKey . 'a'] = [$value[0], $dataType];
        $map[$mapKey . 'b'] = [$value[1], $dataType];

        return "({$column} BETWEEN {$mapKey}a AND {$mapKey}b)";
    }

    private function handleDefaultOperator(string $column, $value, string $type, string $mapKey, array &$map): string
    {
        switch ($type) {
            case 'NULL':
                return $column . ' IS NULL';

            case 'array':
                $values = [];
                foreach ($value as $index => $item) {
                    if ($raw = $this->queryBuilder->buildRaw($item, $map)) {
                        $values[] = $raw;
                    } else {
                        $stackKey = $mapKey . $index . '_i';
                        $values[] = $stackKey;
                        $map[$stackKey] = $this->queryBuilder->typeMap($item, gettype($item));
                    }
                }
                return $column . ' IN (' . implode(', ', $values) . ')';

            case 'object':
                if ($raw = $this->queryBuilder->buildRaw($value, $map)) {
                    return "{$column} = {$raw}";
                }
                break;

            case 'integer':
            case 'double':
            case 'boolean':
            case 'string':
                $map[$mapKey] = $this->queryBuilder->typeMap($value, $type);
                return "{$column} = {$mapKey}";
        }

        return '';
    }

    public function buildClause($where, array &$map, string $type): string
    {
        $clause = '';

        if (is_array($where)) {
            $conditions = array_diff_key($where, array_flip(
                ['GROUP', 'ORDER', 'HAVING', 'LIMIT', 'LIKE', 'MATCH']
            ));

            if (!empty($conditions)) {
                $clause = ' WHERE ' . $this->implode($conditions, $map, ' AND');
            }

            $clause .= $this->buildMatch($where, $map, $clause);
            $clause .= $this->buildGroup($where, $map);
            $clause .= $this->buildHaving($where, $map);
            $clause .= $this->buildOrder($where, $map);
            $clause .= $this->buildLimit($where, $type);
        } elseif ($raw = $this->queryBuilder->buildRaw($where, $map)) {
            $clause .= ' ' . $raw;
        }

        return $clause;
    }

    private function buildMatch(array $where, array &$map, string $clause): string
    {
        if (!isset($where['MATCH'])) {
            return '';
        }

        $match = $where['MATCH'];

        if (!is_array($match) || !isset($match['columns'], $match['keyword'])) {
            return '';
        }

        $mode = '';
        $options = [
            'natural' => 'IN NATURAL LANGUAGE MODE',
            'natural+query' => 'IN NATURAL LANGUAGE MODE WITH QUERY EXPANSION',
            'boolean' => 'IN BOOLEAN MODE',
            'query' => 'WITH QUERY EXPANSION'
        ];

        if (isset($match['mode'], $options[$match['mode']])) {
            $mode = ' ' . $options[$match['mode']];
        }

        $columns = implode(', ', array_map([$this->queryBuilder, 'columnQuote'], $match['columns']));
        $mapKey = $this->queryBuilder->mapKey();
        $map[$mapKey] = [$match['keyword'], PDO::PARAM_STR];

        return ($clause !== '' ? ' AND ' : ' WHERE') . ' MATCH (' . $columns . ') AGAINST (' . $mapKey . $mode . ')';
    }

    private function buildGroup(array $where, array &$map): string
    {
        if (!isset($where['GROUP'])) {
            return '';
        }

        $group = $where['GROUP'];

        if (is_array($group)) {
            $stack = array_map([$this->queryBuilder, 'columnQuote'], $group);
            return ' GROUP BY ' . implode(',', $stack);
        }

        if ($raw = $this->queryBuilder->buildRaw($group, $map)) {
            return ' GROUP BY ' . $raw;
        }

        return ' GROUP BY ' . $this->queryBuilder->columnQuote($group);
    }

    private function buildHaving(array $where, array &$map): string
    {
        if (!isset($where['HAVING'])) {
            return '';
        }

        $having = $where['HAVING'];

        if ($raw = $this->queryBuilder->buildRaw($having, $map)) {
            return ' HAVING ' . $raw;
        }

        return ' HAVING ' . $this->implode($having, $map, ' AND');
    }

    private function buildOrder(array $where, array &$map): string
    {
        if (!isset($where['ORDER'])) {
            return '';
        }

        $order = $where['ORDER'];

        if (is_array($order)) {
            $stack = [];

            foreach ($order as $column => $value) {
                if (is_array($value)) {
                    $valueStack = array_map(
                        fn($item) => is_int($item) ? $item : $this->queryBuilder->quote($item),
                        $value
                    );
                    $stack[] = "FIELD({$this->queryBuilder->columnQuote($column)}, " . implode(',', $valueStack) . ")";
                } elseif ($value === 'ASC' || $value === 'DESC') {
                    $stack[] = $this->queryBuilder->columnQuote($column) . ' ' . $value;
                } elseif (is_int($column)) {
                    $stack[] = $this->queryBuilder->columnQuote($value);
                }
            }

            return ' ORDER BY ' . implode(',', $stack);
        }

        if ($raw = $this->queryBuilder->buildRaw($order, $map)) {
            return ' ORDER BY ' . $raw;
        }

        return ' ORDER BY ' . $this->queryBuilder->columnQuote($order);
    }

    private function buildLimit(array $where, string $type): string
    {
        if (!isset($where['LIMIT'])) {
            return '';
        }

        $limit = $where['LIMIT'];

        if (in_array($type, ['oracle', 'mssql'])) {
            if ($type === 'mssql' && !isset($where['ORDER'])) {
                $clause = ' ORDER BY (SELECT 0)';
            } else {
                $clause = '';
            }

            if (is_numeric($limit)) {
                $limit = [0, $limit];
            }

            if (is_array($limit) && is_numeric($limit[0]) && is_numeric($limit[1])) {
                return $clause . " OFFSET {$limit[0]} ROWS FETCH NEXT {$limit[1]} ROWS ONLY";
            }

            return $clause;
        }

        if (is_numeric($limit)) {
            return ' LIMIT ' . $limit;
        }

        if (is_array($limit) && is_numeric($limit[0]) && is_numeric($limit[1])) {
            return " LIMIT {$limit[1]} OFFSET {$limit[0]}";
        }

        return '';
    }
}
