<?php

namespace MedooOrm;

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
    private $cache;

    public function __construct(array $config, array $schema)
    {
        $this->config = $config;
        $this->schema = $schema;
        $this->connections = [];
        $this->transactions = [];
        $this->cache = [];
    }

    public function getConnection(?string $name = 'default'): MedooWrapper
    {
        if ($conn = $this->connections[$name] ?? false) {
            return $conn;
        }

        $config = $this->config[$name] ?? false;
        if (!$config) {
            throw new \DomainException("Undefined config for table '$name'");
        }

        $conn = new MedooWrapper($config);
        $this->connections[$name] = $conn;

        return $conn;
    }

    public function getConnectionForTable(string $table): MedooWrapper
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

    public function getOrderByForTable(string $table): ?array
    {
        $orderBy = null;
        if ($schema = $this->getSchemaForTable($table)) {
            $orderBy = $schema['order_by'] ?? $orderBy;
        }

        return $orderBy;
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
        $items = $this->getConnectionForTable($table)->select($table, '*', $where + [
            'ORDER' => $this->getOrderByForTable($table)
        ]);

        return new Collection($table, $items, $this);
    }

    public function get(string $table, array $where = []): Item
    {
        $item = $this->getConnectionForTable($table)->get($table, '*', $where + [
            'ORDER' => $this->getOrderByForTable($table)
        ]);

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

    public function metrics(?string $name = null, bool $merge = false): array
    {
        if ($name) {
            return $this->getConnection($name)->metrics();
        }

        $metrics = array_map(function (MedooWrapper $conn) {
            return $conn->metrics();
        }, $this->connections);

        if ($merge) {
            $metrics = array_reduce($metrics, function ($acc, $metric) {
                foreach ($metric as $key => $value) {
                    $acc[$key] = ($acc[$key] ?? 0) + $value;
                }

                return $acc;
            });
        }

        return $metrics;
    }

    public function insert(string $table, array $data)
    {
        $relations = $this->schema[$table]['relations'] ?? [];
        $data = $this->transformData(
            array_diff_key($data, $relations, ['id' => null])
        );

        $connection = $this->getConnectionForTable($table);
        $connection->insert($table, $data);

        return $connection->id();
    }

    public function bulkInsert(string $table, array $data)
    {
        $transformeds = [];
        $relations = $this->schema[$table]['relations'] ?? [];
        foreach ($data as $item) {
            $transformeds[] = $this->transformData(
                array_diff_key((array) $item, $relations, ['id' => null])
            );
        }

        $connection = $this->getConnectionForTable($table);

        return $connection->insert($table, $transformeds);
    }

    public function insertEntity($entity)
    {
        $table =  $this->getTableForEntity($entity);
        // $pk = $this->getPkForTable($table);

        // $relations = $this->schema[$table]['relations'] ?? [];
        // $data = $this->transformData(
        //     array_diff_key((array) $entity, $relations, ['id' => null])
        // );

        $id = $this->insert($table, (array) $entity);
        // $entity->{$pk} = $id;
        $entity->id = (int) $id;

        return $entity;
    }

    public function update(string $table, array $data, array $where)
    {
        $connection = $this->getConnectionForTable($table);

        $relations = $this->schema[$table]['relations'] ?? [];
        $data = $this->transformData(
            array_diff_key($data, $relations, ['id' => null])
        );

        return $connection->update($table, $data, $where);
    }

    public function updateByPk(string $table, array $data, int $id, array $where = [])
    {
        $pk = $this->getPkForTable($table);

        return $this->update($table, $data, $where + [$pk => $id]);
    }

    public function updateEntity($entity)
    {
        $table = $this->getTableForEntity($entity);
        $pk = $this->getPkForTable($table);

        // $this->update($table, $data, [$pk => $entity->{$pk}]);

        $this->update($table, (array) $entity, [$pk => $entity->id]);

        return $entity;
    }

    private function transformData(array $data): array
    {
        foreach ($data as $key => $value) {
            if ($value instanceof \DateTime) {
                $data[$key] = $value->format('c');
            } else {
                $data[$key] = $value;
            }
        }

        return $data;
    }

    public function saveEntity($entity)
    {
        if ($entity->id === null) {
            return $this->insertEntity($entity);
        }

        return $this->updateEntity($entity);
    }

    public function delete(string $table, array $where)
    {
        $connection = $this->getConnectionForTable($table);
        $connection->delete($table, $where);
    }

    public function deleteEntity($entity)
    {
        $table =  $this->getTableForEntity($entity);
        $pk = $this->getPkForTable($table);

        $this->delete($table, [$pk => $entity->id]);

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

    public function cacheGet(string $key)
    {
        if (!isset($this->cache[$key])) {
            [$table, $pk] = explode('.', $key);
            $this->cache[$key] = $this->getByPk($table, $pk)->toEntity();
        }

        return $this->cache[$key] ?? null;
    }
}
