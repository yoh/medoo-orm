<?php

namespace MedooOrm;

class Collection
{
    // public string $table;
    // public array $elements;
    // public array $relations;
    // public Orm $orm;
    public $table;
    public $elements;
    public $relations;
    public $orm;

    public function __construct(string $table, array $elements, Orm $orm)
    {
        $this->table = $table;
        $this->elements = array_filter($elements);
        $this->relations = [];
        $this->orm = $orm;
    }

    public function toArray(): array
    {
        return $this->elements;
    }

    /**
     * @return Array|Object
     */
    public function toEntity()
    {
        return $this->toEntityArray();
    }

    public function toEntityArray(): array
    {
        foreach ($this->relations as &$collection) {
            $collection->toEntityArray();
        }

        $schema = $this->orm->getSchemaForTable($this->table);
        if (isset($schema['entity'])) {
            foreach ($this->elements as $key => $element) {
                $this->elements[$key] = $schema['entity']::fromArray($element);
            }
        }

        return $this->elements;
    }

    /**
     * @return Collection|Item
     */
    public function load(string $relation, array $where = []): self
    {
        $relations = explode('.', $relation);
        if (count($relations) > 1) {
            if (isset($this->relations[$relations[0]])) {
                $relation = array_shift($relations);
                $this->relations[$relation]->load(implode('.', $relations), $where);

                return $this;
            }

            return $this;
        }

        if (!count($this->elements)) {
            return $this;
        }

        $schema = $this->orm->getSchemaForTable($this->table);
        $relDefinition = $schema['relations'][$relation] ?? false;
        if (!$relDefinition) {
            throw new \DomainException("The relation '{$this->table} -> {$relation}' in not defined in the schema");
        }
        $relDefinition->execute($relation, $this, $where);

        return $this;
    }

    public static function pluck(array $items, string $key): array
    {
        return array_unique(array_map(function ($item) use ($key) {
            return is_object($item) ? $item->$key : $item[$key];
        }, $items));
    }

    public static function indexBy(array $items, string $key): array
    {
        return array_combine(self::pluck($items, $key), $items);
    }

    public static function groupBy(array $items, string $key): array
    {
        $groups = [];
        foreach ($items as $item) {
            $groups[$item[$key]] = $groups[$item[$key]] ?? [];
            $groups[$item[$key]][] = $item;
        }

        return $groups;
    }

    public static function extractValueBy(array $items, string $valueKey, string $key): array
    {
        $data = [];
        foreach ($items as $item) {
            $data[$item[$key]] = $item[$valueKey] ?? null;
        }

        return $data;
    }
}
