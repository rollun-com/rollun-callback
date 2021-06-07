<?php
declare(strict_types=1);

namespace rollun\callback\Interfaces;

/**
 * Interface ResultInterface
 *
 * @author r.ratsun <r.ratsun.rollun@gmail.com>
 */
interface ResultInterface
{
    /**
     * Get data
     *
     * @return object|null
     */
    public function getData(): ?object;

    /**
     * Get messages
     *
     * @return MessageInterface[]|null
     */
    public function getMessages(): ?array;
}
