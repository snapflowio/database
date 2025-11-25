<?php

declare(strict_types=1);

namespace Snapflow\Database\Adapters;

interface DatabaseAdapterInterface
{
    public function getType(): string;

    public function getQuotePattern(): string;

    public function getTableAliasConnector(): string;

    public function buildDsn(array $options): string;

    public function getInitCommands(array $options): array;
}
