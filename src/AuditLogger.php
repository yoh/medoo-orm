<?php

namespace MedooOrm;

class AuditLogger
{
    const TYPE_INSERT = 'insert';
    const TYPE_UPDATE = 'update';
    const TYPE_DELETE = 'delete';

    private array $logs = [];

    public function getLogs(): array
    {
        return $this->logs;
    }

    public function getLogsForItem(string $table, $id): array
    {
        return array_filter($this->logs, function (object $log) use ($table, $id) {
            return $log->table === $table && $log->id === $id;
        });
    }

    public function getMergedLogsForItemByTypes(string $table, $id): array
    {
        $byTypes = [
            self::TYPE_INSERT => [],
            self::TYPE_UPDATE => [],
            self::TYPE_DELETE => [],
        ];

        $logs = $this->getLogsForItem($table, $id);
        foreach ($logs as $log) {
            $byTypes[$log->type][] = $log->data;
        }

        return [
            'table' => $table,
            'id' => $id,
            self::TYPE_INSERT => array_merge(...$byTypes[self::TYPE_INSERT]),
            self::TYPE_UPDATE => array_merge(...$byTypes[self::TYPE_UPDATE]),
            self::TYPE_DELETE => array_merge(...$byTypes[self::TYPE_DELETE]),
        ];
    }

    public function getMergedLogs(): array
    {
        $logs = [];
        foreach ($this->logs as $log) {
            $key = "{$log->table}#{$log->id}";
            if (!isset($logs[$key])) {
                $logs[$key] = $this->getMergedLogsForItemByTypes($log->table, $log->id);
            }
        }

        return $logs;
    }

    public function addInsertLog(string $table, $id, array $data): void
    {
        $this->addLog(self::TYPE_INSERT, $table, $id, $data);
    }

    public function addUpdateLog(string $table, $id, array $data): void
    {
        $this->addLog(self::TYPE_UPDATE, $table, $id, $data);
    }

    public function addDeleteLog(string $table, $id, array $data): void
    {
        $this->addLog(self::TYPE_DELETE, $table, $id, $data);
    }

    private function addLog(string $type, string $table, $id, array $data): void
    {
        $this->logs[] = (object) [
            'type' => $type,
            'table' => $table,
            'id' => $id,
            'data' => $data,
        ];
    }
}
