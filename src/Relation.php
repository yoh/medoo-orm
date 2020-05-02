<?php

namespace MedooOrm;

abstract class Relation
{
    // protected string $table;
    // protected string $nativeKey;
    // protected string $foreingKey;
    // protected array $where;
    protected $table;
    protected $nativeKey;
    protected $foreingKey;
    protected $where;

    protected function __construct(
        string $table,
        string $nativeKey,
        string $foreingKey,
        array $where
    ) {
        $this->table = $table;
        $this->nativeKey = $nativeKey;
        $this->foreingKey = $foreingKey;
        $this->where = $where;
    }

    public static function oneToOne(string $related, string $foreignKey, array $where = []): OneToOneRelation
    {
        [$table, $nativeKey] = explode('#', $related);

        return new OneToOneRelation($table, $foreignKey, $nativeKey, $where);
    }

    public static function oneToMany(string $related, string $foreignKey, array $where = []): OneToManyRelation
    {
        [$table, $nativeKey] = explode('#', $related);

        return new OneToManyRelation($table, $foreignKey, $nativeKey, $where);
    }

    public static function manyToOne(string $related, string $foreignKey, array $where = []): ManyToOneRelation
    {
        [$table, $nativeKey] = explode('#', $related);

        return new ManyToOneRelation($table, $foreignKey, $nativeKey, $where);
    }

    public static function manyToMany(string $related, string $join, string $foreignKey, array $where = []): ManyToManyRelation
    {
        [$table, $nativeKey] = explode('#', $related);

        preg_match('/(?<joinKey1>.+)#(?<joinTable>.+)(\((?<indexBy>.+)\))#(?<joinKey>.+)/i', $join, $matches);
        ['joinKey1' => $joinKey1, 'joinTable' => $joinTable, 'indexBy' => $indexBy, 'joinKey' => $joinKey] = $matches;

        return new ManyToManyRelation($table, $nativeKey, $foreignKey, $where, $joinKey1, $joinTable, $joinKey, $indexBy);
    }

    public function execute(string $relation, Collection $collection, array $where = [])
    {
        $relateds = $collection->orm->select($this->table, $where + $this->where + [
            "{$this->foreingKey}[IN]" => Collection::pluck($collection->elements, $this->nativeKey)
        ]);

        if ($this instanceof OneToOneRelation || $this instanceof ManyToOneRelation) {
            $relatedsByIds = Collection::indexBy($relateds->toArray(), $this->foreingKey);
            foreach ($collection->elements as &$element) {
                $element[$relation] = &$relatedsByIds[$element[$this->nativeKey]];
            }

            $collection->relations[$relation] = new Collection($this->table, $relatedsByIds, $collection->orm);
        } else if ($this instanceof OneToManyRelation) {
            foreach ($collection->elements as &$element) {
                $element[$relation] = $element[$relation] ?? [];
            }

            $elementsByIds = Collection::indexBy($collection->elements, $this->nativeKey);
            foreach ($relateds->elements as &$related) {
                $element = &$elementsByIds[$related[$this->foreingKey]];
                $element[$relation][] = &$related;
            }

            $collection->elements = array_values($elementsByIds);
            $collection->relations[$relation] = $relateds;
        }
    }
}

class OneToOneRelation extends Relation{}
class OneToManyRelation extends Relation{}
class ManyToOneRelation extends Relation{}
class ManyToManyRelation extends Relation
{
    // protected string $joinKey1;
    // protected string $joinTable;
    // protected string $joinKey;
    // protected string $indexedBy;
    protected $joinKey1;
    protected $joinTable;
    protected $joinKey;
    protected $indexedBy;

    public function __construct(
        string $table,
        string $nativeKey,
        string $foreingKey,
        array $where,
        string $joinKey1,
        string $joinTable,
        string $joinKey,
        string $indexedBy
    ) {
        parent::__construct($table, $nativeKey, $foreingKey, $where);

        $this->joinKey1 = $joinKey1;
        $this->joinTable = $joinTable;
        $this->joinKey = $joinKey;
        $this->indexedBy = $indexedBy;
    }

    public function execute(string $relation, Collection $collection, array $where = [])
    {
        $joins = $collection->orm->getConnectionForTable($this->joinTable)->select($this->joinTable, '*', [
            "{$this->joinKey}[IN]" => Collection::pluck($collection->elements, $this->foreingKey)
        ]);
        $relateds = $collection->orm->select($this->table, $where + $this->where + [
            "{$this->nativeKey}[IN]" => Collection::pluck($joins, $this->joinKey1)
        ]);

        $relatedsByJoinKey1 = Collection::indexBy($relateds->elements, $this->nativeKey);
        foreach ($collection->elements as $k => $element) {
            $joinsForElement = array_filter($joins, function(array $join) use ($element){
                return $join[$this->joinKey] == $element[$this->foreingKey];
            });

            $collection->elements[$k][$relation] = $collection->elements[$k][$relation] ?? [];
            foreach ($joinsForElement as $join) {
                $related = &$relatedsByJoinKey1[$join[$this->joinKey1]];
                $collection->elements[$k][$relation][$join[$this->indexedBy] ?? null] = &$related;
            }
        }

        $relateds->elements = array_values($relatedsByJoinKey1);
        $collection->relations[$relation] = $relateds;
    }
}
