<?php

namespace MedooOrm;

abstract class AbstractRepository
{
    // protected Orm $orm;
    protected $orm;

    public function __construct(Orm $orm)
    {
        $this->orm = $orm;
    }
}
