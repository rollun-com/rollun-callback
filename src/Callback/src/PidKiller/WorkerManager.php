<?php
/**
 * @copyright Copyright © 2014 Rollun LC (http://rollun.com/)
 * @license LICENSE.md New BSD License
 */

namespace rollun\callback\PidKiller;

use phpDocumentor\Reflection\Types\This;
use Jaeger\Tag\StringTag;
use Jaeger\Tracer\Tracer;
use Psr\Log\LoggerInterface;
use Psr\SimpleCache\CacheInterface;
use rollun\callback\Callback\Interrupter\InterrupterInterface;
use rollun\callback\Callback\Interrupter\Process;
use rollun\dic\InsideConstruct;
use rollun\utils\Json\Serializer;
use Zend\Cache\Storage\StorageInterface;
use Zend\Db\ResultSet\ResultSetInterface;
use Zend\Db\TableGateway\TableGateway;


class WorkerManager
{
    /**
     * @var LoggerInterface
     */
    protected $logger;

    /**
     * @var Tracer
     */
    protected $tracer;

    /**
     * @var Process
     */
    private $interrupter;

    /**
     * @var string
     */
    private $workerManagerName;


    /**
     * @var int
     */
    private $processCount;

    /**
     * @var ProcessManager
     */
    private $processManager;

    /**
     * @var int
     */
    private $slotTakenSecondsLimit;

    /**
     * @var StorageInterface
     */
    private $slotCache;

    /**
     * WorkerManager constructor.
     * @param StorageInterface $slotCache
     * @param InterrupterInterface $interrupter
     * @param string $workerManagerName
     * @param int $processCount
     * @param ProcessManager|null $processManager
     * @param float|int $slotTakenSecondsLimit
     * @param LoggerInterface|null $logger
     * @param Tracer|null $tracer
     * @throws \ReflectionException
     */
    public function __construct(
        StorageInterface $slotCache,
        InterrupterInterface $interrupter,
        string $workerManagerName,
        int $processCount,
        ProcessManager $processManager = null,
        $slotTakenSecondsLimit = 1800,
        LoggerInterface $logger = null,
        Tracer $tracer = null
    ) {
        InsideConstruct::setConstructParams([
            'tracer' => Tracer::class,
            'logger' => LoggerInterface::class,
            'processManager' => ProcessManager::class,
        ]);
        $this->interrupter = $interrupter;
        $this->setWorkerManagerName($workerManagerName);
        $this->processCount = $processCount;
        $this->processManager = $processManager ?? new ProcessManager();
        $this->slotTakenSecondsLimit = $slotTakenSecondsLimit;
        $this->slotCache = $slotCache;
    }

    private function setWorkerManagerName($workerManagerName)
    {
        if (!$workerManagerName) {
            throw new \InvalidArgumentException('Worker manager name is invalid (empty)');
        }

        $this->workerManagerName = $workerManagerName;
    }

    /**
     * @return array|mixed
     * @throws \Psr\SimpleCache\InvalidArgumentException
     * @throws \rollun\utils\Json\Exception
     */
    public function __invoke()
    {
        $span = $this->tracer->start('WorkerManager::__invoke', [new StringTag('name', $this->workerManagerName)]);

        $slots = Serializer::jsonUnserialize($this->slotCache->getItem('slots'));

        $freeSlots = $this->setupSlots($slots);

        if (!$freeSlots) {
            $this->logger->debug('All slots are in working');
        }

        $slots = [];

        foreach ($freeSlots as $id => $freeSlot) {
            $slot = $this->refreshSlot($freeSlot);
            if ($slots === null) {
                unset($slots[$id]);
            } else {
                $slots[$id] = $slot;
            }
        }

        $this->slotCache->setItem('slots', Serializer::jsonSerialize($slots));

        $span->addTag(new StringTag('slots', Serializer::jsonSerialize($slots)));
        $this->tracer->finish($span);
        return $slots;
    }

    private function refreshSlot($slot)
    {
        $span = $this->tracer->start('WorkerManager::refreshSlot', [
            new StringTag('name', $this->workerManagerName),
            new StringTag('slots', json_encode($slot))
        ]);

        try {
            $payload = $this->interrupter->__invoke();
            $info = $this->processManager->pidInfo($payload->getId());
            $slot = array_merge($slot, [
                'pid' => $payload->getId(),
                'pid_id' => $info['id']
            ]);

            $this->logger->debug("Update slot with pid = {$payload->getId()} where id = {$slot['id']}");
        } catch (\Throwable $e) {
            $this->logger->error('Failed update slot', ['exception' => $e]);
            return null;
        }
        $this->tracer->finish($span);

        return $slot;
    }


    /**
     * Get array of killed processes
     *
     * @return array
     * @throws \Psr\SimpleCache\InvalidArgumentException
     */
    private function setupSlots($slots): array
    {
        $span = $this->tracer->start('WorkerManager::setupSlots', [new StringTag('name', $this->workerManagerName)]);

        $freeSlots = $this->receiveFreeSlots($slots);
        if (count($slots) < $this->processCount) {
            //create if not exists
            for ($i = count($slots); $i < $this->processCount; $i++) {
                $newSlot = [
                    'id' => uniqid($this->workerManagerName, true),
                    'pid' => '',
                    'pid_id' => '',
                    'worker_manager' => $this->workerManagerName,
                ];
                $freeSlots[$newSlot['id']] = $newSlot;
            }
        } elseif (count($slots) > $this->processCount) {
            //remove if more slots
            for ($slot = current($freeSlots), $i = count($slots), $slotSkip = 0; $i > $this->processCount; $i--, $slot = next($freeSlots), $slotSkip++) {
                if (false !== $slot) {
                    unset($slots[$slot['id']]);
                } else {
                    //No free slot left.
                    return [];
                }
            }
            $freeSlots = array_slice($freeSlots, $slotSkip);
        }
        $this->tracer->finish($span);
        return $freeSlots;
    }

    /**
     * @param $slots
     * @return array
     */
    private function receiveFreeSlots($slots): array
    {
        $span = $this->tracer->start('WorkerManager::receiveFreeSlots', [new StringTag('name', $this->workerManagerName)]);
        $existingPids = $this->processManager->ps();
        $freeSlots = [];
        foreach ($slots as $slot) {
            $isSlotFree = true;

            foreach ($existingPids as $pidInfo) {
                if ($pidInfo['id'] === $slot['pid_id']) {
                    $isSlotFree = false;
                }
            }

            if ($isSlotFree) {
                $freeSlots[] = (array)$slot;
            } else {
                //FIXME: not good practice use id for get from it data
                $startTaskTime = str_replace("{$slot['pid']}.", '', $slot['pid_id']);
                $workSeconds = time() - $startTaskTime;
                if ($workSeconds > $this->slotTakenSecondsLimit) {
                    $this->logger->emergency('Slot busy longer than allowed time.', [
                        'slot' => $slot,
                        'workerManagerName' => $this->workerManagerName
                    ]);
                }
            }
        }

        $this->tracer->finish($span);
        return $freeSlots;
    }

    public function __sleep()
    {
        return ['interrupter', 'workerManagerName', 'processCount', 'slotCache', 'processManager', 'slotTakenSecondsLimit'];
    }

    public function __wakeup()
    {
        InsideConstruct::initWakeup([
            'logger' => LoggerInterface::class,
            'tracer' => Tracer::class,
        ]);
    }
}
