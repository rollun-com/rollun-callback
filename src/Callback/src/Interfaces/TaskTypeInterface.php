<?php
declare(strict_types=1);

namespace rollun\callback\Interfaces;

/**
 * Interface TaskTypeInterface
 *
 * @author r.ratsun <r.ratsun.rollun@gmail.com>
 */
interface TaskTypeInterface
{
    /**
     * Get mat task timeout
     *
     * @return int
     */
    public function getTimeout(): int;

    /**
     * Get all possible task type statuses
     *
     * @return string[]
     */
    public function getAllStatuses(): array;

    /**
     * Get all possible task type stages
     *
     * @return string[]
     */
    public function getAllStages(): array;

    /**
     * Count of tasks in pending status
     *
     * @return int
     */
    public function getPendingCount(): int;
}
