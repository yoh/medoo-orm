<?php

namespace MedooOrm;

use Medoo\Medoo;

class MedooWrapper extends Medoo
{
    private $metrics = [
        'SELECT' => 0,
        'INSERT' => 0,
        'UPDATE' => 0,
        'DELETE' => 0,
        'OTHER' => 0,
    ];

    public function exec($query, $map = [])
    {
        $idx = 'OTHER';

        if (stripos($query, 'SELECT') === 0) {
            $idx = 'SELECT';
        } else if (stripos($query, 'UPDATE') === 0) {
            $idx = 'UPDATE';
        } else if (stripos($query, 'INSERT') === 0) {
            $idx = 'INSERT';
        } else if (stripos($query, 'DELETE') === 0) {
            $idx = 'DELETE';
        }

        $this->metrics[$idx]++;

        return parent::exec($query, $map);
    }

    public function metrics(): array
    {
        return $this->metrics;
    }

    /**
     * Medoo binds the values of a condition but writes the keys of an array value into the SQL:
     * they name the placeholders of an `IN (…)` or of a multiple LIKE, whose first key also
     * becomes its connector. Only the position of a value matters, so an array with string keys
     * is reindexed before Medoo builds the clause.
     */
    protected function dataImplode($data, &$map, $conjunctor)
    {
        foreach ($data as $key => $value) {
            // Medoo calls dataImplode() again on an AND / OR group: it is handled on that pass
            $isGroup = is_string($key) && preg_match('/^(AND|OR)(\s+#.*)?$/', $key);
            // a list with integer keys is left untouched
            if (!is_array($value) || $isGroup || !array_filter(array_keys($value), 'is_string')) {
                continue;
            }

            $data[$key] = array_values($value);
            // the multiple LIKE form `['AND' => […]]` / `['OR' => […]]` keeps its connector
            foreach (['AND', 'OR'] as $connector) {
                if (is_array($value[$connector] ?? null)) {
                    $data[$key] = [$connector => array_values($value[$connector])];
                    break;
                }
            }
        }

        return parent::dataImplode($data, $map, $conjunctor);
    }
}
