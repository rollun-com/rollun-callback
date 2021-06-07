<?php
declare(strict_types=1);

namespace rollun\callback\Interfaces;

/**
 * Interface TaskInterface
 *
 * @author r.ratsun <r.ratsun.rollun@gmail.com>
 */
interface TaskInterface
{
    /**
     * Get task id
     *
     * @return string
     */
    public function getId(): string;

    /**
     * Get task current status
     *
     * @return string
     */
    public function getStatus(): string;

    /**
     * Get task current stage
     *
     * @return string
     */
    public function getStage(): string;

    /**
     * Get datetime when task was start
     *
     * @return \DateTime|null
     */
    public function getStartTime(): ?\DateTime;
}
