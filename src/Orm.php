<?php

namespace MedooOrm;

use Medoo\Medoo;

class Orm
{
    // private array $config;
    // private array $schema;
    // private array $connections;
    // private bool $transactions;
    private $config;
    private $schema;
    private $connections;
    private $transactions;

    public function __construct(array $config, array $schema)
    {
        $this->config = $config;
        $this->schema = $schema;
        $this->connections = [];
        $this->transactions = [];
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

    public function getTableForEntity($entity): string
    {
        $fcqn = get_class($entity);
        foreach ($this->schema as $table => $schema) {
            if ($schema['entity'] === $fcqn) {
                return $table;
            }
        }

        throw new \DomainException("Undefined table for entity '{$fcqn}'");
    }

    public function getPkForTable(string $table): string
    {
        $pk = 'id';
        if ($schema = $this->getSchemaForTable($table)) {
            $pk = $schema['pk'] ?? $pk;
        }

        return $pk;
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

    public function getByPk(string $table, $id): Item
    {
        $pk = $this->getPkForTable($table);

        return $this->get($table, [$pk => $id]);
    }

    public function selectByPks(string $table, array $ids): Collection
    {
        $pk = $this->getPkForTable($table);

        return $this->select($table, [$pk => $ids]);
    }

    public function log(?string $name = null): array
    {
        if ($name) {
            return $this->getConnection($name)->log();
        }

        return array_map(function ($conn) {
            return $conn->log();
        }, $this->connections);
    }

    public function insert(string $table, array $data)
    {
        $connection = $this->getConnectionForTable($table);
        $connection->insert($table, $data);

        return $connection->id();
    }

    public function insertEntity($entity)
    {
        $table =  $this->getTableForEntity($entity);
        $pk = $this->getPkForTable($table);

        $relations = $this->schema[$table]['relations'] ?? [];
        $data = array_diff_key((array) $entity, $relations);

        $id = $this->insert($table, $data);
        $entity->{$pk} = $id;

        return $entity;
    }

    public function update(string $table, array $data, array $where)
    {
        $connection = $this->getConnectionForTable($table);
        $connection->update($table, $data, $where);
    }

    public function updateEntity($entity)
    {
        $table =  $this->getTableForEntity($entity);
        $pk = $this->getPkForTable($table);

        $relations = $this->schema[$table]['relations'] ?? [];
        $data = array_diff_key((array) $entity, $relations);
        $this->update($table, $data, [$pk => $entity->{$pk}]);

        return $entity;
    }

    public function beginTransaction(?string $name = 'default')
    {
        if ($this->transactions[$name] ?? false) {
            throw new \LogicException("Unable to start transaction inside another transaction on the same connection");
        }

        $connection = $this->getConnection($name);
        $this->transactions[$name] = $connection->pdo->beginTransaction();
    }

    public function commit(?string $name = 'default')
    {
        $connection = $this->getConnection($name);
        $connection->pdo->commit();
        $this->transactions[$name] = false;
    }

    public function rollback(?string $name = 'default')
    {
        $connection = $this->getConnection($name);
        $connection->pdo->rollback();
        $this->transactions[$name] = false;
    }
}
