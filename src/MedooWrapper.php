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
}
