<?php

namespace MedooOrm;

use Medoo\Medoo;

class Orm
{
    // private array $config;
    // private array $schema;
    // private array $connections;
    private $config;
    private $schema;
    private $connections;

    public function __construct(array $config, array $schema)
    {
        $this->config = $config;
        $this->schema = $schema;
        $this->connections = [];
    }

    public function getConnection(?string $name = 'default'): Medoo
    {
        if ($conn = $this->connections[$name] ?? false) {
            return $conn;
        }

        $config = $this->config[$name] ?? false;
        if (!$config) {
            throw new \DomainException("Undefined config for table '$name'");
        }

        $conn = new Medoo($config);
        $this->connections[$name] = $conn;

        return $conn;
    }

    public function getConnectionForTable(string $table): Medoo
    {
        return $this->getConnection($this->schema[$table]['connection'] ?? 'default');
    }

    public function getSchemaForTable(string $table): array
    {
        if ($schema = $this->schema[$table] ?? false) {
            return $schema;
        }

        return [$table => []];
        throw new \DomainException("Undefined schema for table '$table'");
    }

    public function repo(string $table): AbstractRepository
    {
        if ($repository = $this->schema[$table]['repository'] ?? false) {
            return new $repository($this);
        }

        throw new \DomainException("Undefined repository for table '$table'");
    }

    public function select(string $table, array $where = []): Collection
    {
        $items = $this->getConnectionForTable($table)->select($table, '*', $where);

        return new Collection($table, $items, $this);
    }

    public function get(string $table, array $where = []): Item
    {
        $item = $this->getConnectionForTable($table)->get($table, '*', $where);

        return new Item($table, [$item], $this);
    }

    public function getById(string $table, int $id): Item
    {
        $pk = 'id';
        if ($schema = $this->getSchemaForTable($table)) {
            $pk = $schema['pk'] ?? $pk;
        }

        return $this->get($table, [$pk => $id]);
    }

    public function selectByIds(string $table, array $ids): Collection
    {
        $pk = 'id';
        if ($schema = $this->getSchemaForTable($table)) {
            $pk = $schema['pk'] ?? $pk;
        }

        return $this->select($table, [$pk => $ids]);
    }

    public function log(?string $name = null): array
    {
        if ($name) {
            return $this->getConnection($name)->log();
        }

        return array_map(fn($conn) => $conn->log(), $this->connections);
    }
}
