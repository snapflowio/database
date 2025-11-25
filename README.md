# Snapflow Database

A lightweight and simple database abstraction framework for Postgres and PHP.

## Installation

```bash
composer require snapflowio/database
```

## Basic Usage

```php
use Snapflow\Database\Database;

// Connect to database
$database = new Database([
    'type' => 'pgsql',
    'host' => 'localhost',
    'database' => 'test_db',
    'username' => 'postgres',
    'password' => 'password'
]);

// Select data
$users = $database->select('users', '*');

// Insert data
$database->insert('users', [
    'user_name' => 'John Doe',
    'email' => 'john@example.com'
]);
```

## License

This library is available under the MIT License.

## Copyright

```
Copyright (c) 2025 Snapflow
```