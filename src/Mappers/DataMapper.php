<?php

declare(strict_types=1);

namespace Snapflow\Database\Mappers;

use Snapflow\Database\Raw;

class DataMapper
{
    private const TABLE_PATTERN = "[\p{L}_][\p{L}\p{N}@$#\-_]*";
    private const COLUMN_PATTERN = "[\p{L}_][\p{L}\p{N}@$#\-_\.]*";

    public function __construct(
        private bool $isRawCallback
    )
    {
    }

    private function isRaw($object): bool
    {
        return $object instanceof Raw;
    }

    public function map(
        array  $data,
        array  $columns,
        array  $columnMap,
        array  &$stack,
        bool   $root,
        ?array &$result = null
    ): void
    {
        if ($root) {
            $columnsKey = array_keys($columns);

            if (count($columnsKey) === 1 && is_array($columns[$columnsKey[0]])) {
                $indexKey = array_keys($columns)[0];
                $dataKey = preg_replace("/^" . self::COLUMN_PATTERN . "\./u", '', $indexKey);
                $currentStack = [];

                foreach ($data as $item) {
                    $this->map($data, $columns[$indexKey], $columnMap, $currentStack, false, $result);
                    $index = $data[$dataKey];

                    if (isset($result)) {
                        $result[$index] = $currentStack;
                    } else {
                        $stack[$index] = $currentStack;
                    }
                }
            } else {
                $currentStack = [];
                $this->map($data, $columns, $columnMap, $currentStack, false, $result);

                if (isset($result)) {
                    $result[] = $currentStack;
                } else {
                    $stack = $currentStack;
                }
            }

            return;
        }

        foreach ($columns as $key => $value) {
            $isRaw = $this->isRaw($value);

            if (is_int($key) || $isRaw) {
                $map = $columnMap[$isRaw ? $key : $value];
                $columnKey = $map[0];
                $item = $data[$columnKey];

                if (isset($map[1])) {
                    if ($isRaw && in_array($map[1], ['Object', 'JSON'])) {
                        continue;
                    }

                    if (is_null($item)) {
                        $stack[$columnKey] = null;
                        continue;
                    }

                    $stack[$columnKey] = match ($map[1]) {
                        'Number' => (float)$item,
                        'Int' => (int)$item,
                        'Bool' => (bool)$item,
                        'Object' => unserialize($item),
                        'JSON' => json_decode($item, true),
                        'String' => (string)$item,
                        default => $item
                    };
                } else {
                    $stack[$columnKey] = $item;
                }
            } else {
                $currentStack = [];
                $this->map($data, $value, $columnMap, $currentStack, false, $result);
                $stack[$key] = $currentStack;
            }
        }
    }
}
