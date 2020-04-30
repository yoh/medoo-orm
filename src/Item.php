<?php

namespace MedooOrm;

class Item extends Collection
{
    public function toEntity()
    {
        return $this->toEntityArray()[0] ?? null;
    }
}
