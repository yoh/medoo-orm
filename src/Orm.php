<?php

namespace MedooOrm;

use Medoo\Raw;

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
    private $autoSnapshot;
    private $snapshots;
    private $auditLogger;

    public function __construct(array $config, array $schema)
    {
        $this->config = $config;
        $this->schema = $schema;
        $this->connections = [];
        $this->transactions = [];
        $this->cache = [];
        $this->autoSnapshot = false;
        $this->snapshots = [];
        $this->auditLogger = new AuditLogger;
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

    public function isTableAuditLogged(string $table): bool
    {
        return $this->schema[$table]['audit_log'] ??
            $this->schema['*']['audit_log'] ??
            false;
    }

    public function getTableForEntity($entity): string
    {
        return $this->getTableForEntityFqcn(get_class($entity));
    }

    public function getTableForEntityFqcn(string $fqcn): string
    {
        foreach ($this->schema as $table => $schema) {
            if (
                $table !== '*' &&
                $schema['entity'] === $fqcn
            ) {
                return $table;
            }
        }

        throw new \DomainException("Undefined table for entity '{$fqcn}'");
    }

    public function getEntityForTable(string $table): string
    {
        return $this->getSchemaForTable($table)['entity'] ?? null;
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

        return (array) $metrics;
    }

    public function insert(string $table, array $data)
    {
        $data = $this->transformData($table, $data);
        $connection = $this->getConnectionForTable($table);
        $connection->insert($table, $data);
        $id = $connection->id();

        if ($this->isTableAuditLogged($table)) {
            $this->auditLogger->addInsertLog($table, $id, $data);
        }

        return $id;
    }

    public function bulkInsert(string $table, array $data)
    {
        $transformeds = [];
        foreach ($data as $item) {
            $transformeds[] = $this->transformData($table, (array) $item);
        }

        $connection = $this->getConnectionForTable($table);

        return $connection->insert($table, $transformeds);
    }

    public function insertEntity($entity)
    {
        $table =  $this->getTableForEntity($entity);

        $data = $this->transformData($table, (array) $entity);
        $id = $this->insert($table, $data);
        $entity->id = (int) $id;

        return $entity;
    }

    public function update(string $table, array $data, array $where, bool $alreadyTransformed = false)
    {
        $connection = $this->getConnectionForTable($table);

        if (!$alreadyTransformed) {
            $data = $this->transformData($table, $data);
        }

        return $connection->update($table, $data, $where);
    }

    public function updateByPk(string $table, array $data, int $id, array $where = [])
    {
        $pk = $this->getPkForTable($table);
        $data = $this->transformData($table, $data);

        // get or take entity snapshot if needed
        $snapshot = $this->getSnapshot($table, $id);
        if (!$snapshot) {
            $entity = $this->getByPk($table, $id)->toEntity();
            $this->takeEntitySnapshot($entity);
            $snapshot = $this->getSnapshot($table, $id);
        }

        // use snapshot to get diffs
        if ($snapshot) {
            $diffs = [];
            foreach ($data as $key => $value) {
                if (!array_key_exists($key, $snapshot) || $snapshot[$key] !== $value) {
                    $diffs[$key] = $value;
                }
            }
            $data = $diffs;
        }

        // save data or diffs
        $result = null;
        if (count($data)) {
            $result = $this->update($table, $data, $where + [$pk => $id], true);
            if ($this->isTableAuditLogged($table)) {
                $this->auditLogger->addUpdateLog($table, $id, $data);
            }

            if ($snapshot) {
                $this->snapshots["$table#{$id}#current"] = array_merge($snapshot, $data);
            }
        }

        return $result;
    }

    public function updateEntity($entity)
    {
        $table = $this->getTableForEntity($entity);
        $this->updateByPk($table, (array) $entity, $entity->id);

        return $entity;
    }

    private function transformData(string $table, array $data): array
    {
        $relations = $this->schema[$table]['relations'] ?? [];
        $data = array_diff_key($data, $relations, ['id' => null]);

        // custom data transformer
        $dataTransformer = $this->schema[$table]['data_transformer'] ?? null;
        if ($dataTransformer) {
            $data = $dataTransformer($data) + $data;
        }

        // default data transformer
        foreach ($data as $key => $value) {
            if ($value instanceof \DateTime) {
                $data[$key] = $value->format('c');
            } else if ($value instanceof Raw) {
                $data[$key] = $value;
            } else if (!is_null($value) && !is_scalar($value)) {
                $data[$key] = json_encode($value);
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
        if ($this->isTableAuditLogged($table)) {
            $data = $this->transformData($table, (array) $entity);
            $this->auditLogger->addDeleteLog($table, $entity->id, $data);
        }

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

    public function setAutoSnapshot(bool $autoSnapshot): void
    {
        $this->autoSnapshot = $autoSnapshot;
    }

    public function isAutoSnapshot(): bool
    {
        return $this->autoSnapshot;
    }

    public function takeEntitySnapshot($entity): void
    {
        $table = $this->getTableForEntity($entity);
        $data = $this->transformData($table, (array) $entity);

        $this->snapshots["$table#{$entity->id}"] = $data;
    }

    public function getSnapshots(): array
    {
        return $this->snapshots;
    }

    public function getSnapshot(string $table, $id, bool $withCurrent = true): ?array
    {
        $key = "$table#$id";

        $current = null;
        if ($withCurrent) {
            $current = $this->snapshots["$key#current"] ?? null;
        }

        return
            $current ??
            $this->snapshots[$key] ??
            null;
    }

    public function getEntitySnapshot($entity, bool $withCurrent = true): ?array
    {
        $table = $this->getTableForEntity($entity);

        return $this->getSnapshot($table, $entity->id, $withCurrent);
    }

    public function getAuditLogger(): AuditLogger
    {
        return $this->auditLogger;
    }

    public function getAuditLogs(): array
    {
        $logs = $this->auditLogger->getMergedLogs();
        foreach ($logs as $key => $log) {
            $logs[$key] = $this->computeAuditLog($log);
        }

        return $logs;
    }

    private function computeAuditLog(array $log): array
    {
        $table = $log['table'];
        $id = $log['id'];
        $log['entity'] = $this->getEntityForTable($table);

        $snapshot = $this->snapshots["{$table}#{$id}"] ?? null;
        foreach ($log[AuditLogger::TYPE_UPDATE] as $prop => $newValue) {
            $log[AuditLogger::TYPE_UPDATE][$prop] = [
                $snapshot[$prop] ?? null,
                $newValue
            ];
        }

        return $log;
    }
}
