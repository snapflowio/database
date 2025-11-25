<?php

declare(strict_types=1);

namespace Snapflow\Database\Builders;

use InvalidArgumentException;
use Snapflow\Database\Raw;

class ColumnBuilder
{
    private const TABLE_PATTERN = "[\p{L}_][\p{L}\p{N}@$#\-_]*";
    private const COLUMN_PATTERN = "[\p{L}_][\p{L}\p{N}@$#\-_\.]*";
    private const ALIAS_PATTERN = "[\p{L}_][\p{L}\p{N}@$#\-_]*";

    public function __construct(
        private QueryBuilder $queryBuilder
    )
    {
    }

    public function push(&$columns, array &$map, bool $root, bool $isJoin = false): string
    {
        if ($columns === '*') {
            return $columns;
        }

        $stack = [];
        $hasDistinct = false;

        if (is_string($columns)) {
            $columns = [$columns];
        }

        foreach ($columns as $key => $value) {
            $isIntKey = is_int($key);
            $isArrayValue = is_array($value);

            if (!$isIntKey && $isArrayValue && $root && count(array_keys($columns)) === 1) {
                $stack[] = $this->queryBuilder->columnQuote($key);
                $stack[] = $this->push($value, $map, false, $isJoin);
            } elseif ($isArrayValue) {
                $stack[] = $this->push($value, $map, false, $isJoin);
            } elseif (!$isIntKey && $raw = $this->queryBuilder->buildRaw($value, $map)) {
                preg_match("/(?<column>" . self::COLUMN_PATTERN . ")(\s*\[(?<type>(String|Bool|Int|Number))\])?/u", $key, $match);
                $stack[] = "{$raw} AS {$this->queryBuilder->columnQuote($match['column'])}";
            } elseif ($isIntKey && is_string($value)) {
                if ($isJoin && strpos($value, '*') !== false) {
                    throw new InvalidArgumentException('Cannot use table.* to select all columns while joining table.');
                }

                preg_match("/(?<column>" . self::COLUMN_PATTERN . ")(?:\s*\((?<alias>" . self::ALIAS_PATTERN . ")\))?(?:\s*\[(?<type>(?:String|Bool|Int|Number|Object|JSON))\])?/u", $value, $match);

                $columnString = '';

                if (!empty($match['alias'])) {
                    $columnString = "{$this->queryBuilder->columnQuote($match['column'])} AS {$this->queryBuilder->columnQuote($match['alias'])}";
                    $columns[$key] = $match['alias'];

                    if (!empty($match['type'])) {
                        $columns[$key] .= ' [' . $match['type'] . ']';
                    }
                } else {
                    $columnString = $this->queryBuilder->columnQuote($match['column']);
                }

                if (!$hasDistinct && strpos($value, '@') === 0) {
                    $columnString = 'DISTINCT ' . $columnString;
                    $hasDistinct = true;
                    array_unshift($stack, $columnString);
                    continue;
                }

                $stack[] = $columnString;
            }
        }

        return implode(',', $stack);
    }

    public function map($columns, array &$stack, bool $root): array
    {
        if ($columns === '*') {
            return $stack;
        }

        foreach ($columns as $key => $value) {
            if (is_int($key)) {
                preg_match("/(" . self::TABLE_PATTERN . "\.)?(?<column>" . self::COLUMN_PATTERN . ")(?:\s*\((?<alias>" . self::ALIAS_PATTERN . ")\))?(?:\s*\[(?<type>(?:String|Bool|Int|Number|Object|JSON))\])?/u", $value, $keyMatch);

                $columnKey = !empty($keyMatch['alias']) ? $keyMatch['alias'] : $keyMatch['column'];

                $stack[$value] = isset($keyMatch['type']) ? [$columnKey, $keyMatch['type']] : [$columnKey];
            } elseif ($value instanceof Raw) {
                preg_match("/(" . self::TABLE_PATTERN . "\.)?(?<column>" . self::COLUMN_PATTERN . ")(\s*\[(?<type>(String|Bool|Int|Number))\])?/u", $key, $keyMatch);
                $columnKey = $keyMatch['column'];

                $stack[$key] = isset($keyMatch['type']) ? [$columnKey, $keyMatch['type']] : [$columnKey];
            } elseif (!is_int($key) && is_array($value)) {
                if ($root && count(array_keys($columns)) === 1) {
                    $stack[$key] = [$key, 'String'];
                }

                $this->map($value, $stack, false);
            }
        }

        return $stack;
    }
}
