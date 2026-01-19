<?php

namespace MedooOrm;

/**
 * @template T
 */
abstract class AbstractRepository
{
    protected Orm $orm;
    protected string $table;

    public function __construct(Orm $orm, string $table)
    {
        $this->orm = $orm;
        $this->table = $table;
    }

    /** @return Collection<T> */
    public function select(array $where = []): Collection
    {
        return $this->orm->select($this->table, $where);
    }

    /** @return Item<T> */
    public function get(array $where = []): Item
    {
        return $this->orm->get($this->table, $where);
    }

    /** @return Item<T> */
    public function getByPk($id): Item
    {
        return $this->orm->getByPk($this->table, $id);
    }

    /** @return Collection<T> */
    public function selectByPks(array $ids): Collection
    {
        return $this->orm->selectByPks($this->table, $ids);
    }

    public function insert(array $data)
    {
        return $this->orm->insert($this->table, $data);
    }

    public function bulkInsert(array $data)
    {
        return $this->orm->bulkInsert($this->table, $data);
    }

    public function update(array $data, array $where, bool $alreadyTransformed = false)
    {
        return $this->orm->update($this->table, $data, $where, $alreadyTransformed);
    }

    public function updateByPk(array $data, int $id, array $where = [])
    {
        return $this->orm->updateByPk($this->table, $data, $id, $where);
    }

    public function delete(array $where)
    {
        return $this->orm->delete($this->table, $where);
    }

    public function cacheGetById(string $id)
    {
        return $this->orm->cacheGet("{$this->table}.{$id}");
    }
}
