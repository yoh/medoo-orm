<?php

namespace MedooOrm;

/**
 * @template T
 */
class Item extends Collection
{
    /** @return T|null */
    public function toEntity()
    {
        return $this->toEntityArray()[0] ?? null;
    }
}
