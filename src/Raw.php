<?php

declare(strict_types=1);

namespace Snapflow\Database;

class Raw
{
    public array $map;
    public string $value;

    public function __construct(string $value = '', array $map = [])
    {
        $this->value = $value;
        $this->map = $map;
    }
}
