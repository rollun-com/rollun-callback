<?php
/**
 * @copyright Copyright © 2014 Rollun LC (http://rollun.com/)
 * @license LICENSE.md New BSD License
 */

namespace rollun\callback\PidKiller;

use DateTime;
use GuzzleHttp\Tests\Stream\Str;
use InvalidArgumentException;
use Jaeger\Tag\ErrorTag;
use Jaeger\Tag\StringTag;
use Jaeger\Tracer\Tracer;
use Psr\Log\LoggerInterface;
use rollun\callback\ConfigProvider;
use rollun\callback\Queues\Message;
use rollun\callback\Queues\QueueInterface;
use rollun\dic\InsideConstruct;
use RuntimeException;

class LinuxPidKiller implements PidKillerInterface
{
    public const DEF_MAX_MESSAGE_COUNT = 1000;

    /** @var LoggerInterface */
    protected $logger;

    /** @var int */
    private $maxMessageCount;

    /** @var QueueInterface */
    private $pidKillerQueue;

    /**
     * @var ProcessManager
     */
    private $processManager;

    /**
     * @var Tracer
     */
    private $tracer;

    /**
     * LinuxPidKiller constructor.
     * @param null $maxMessageCount
     * @param ProcessManager|null $processManager
     * @param QueueInterface|null $pidKillerQueue
     * @param LoggerInterface|null $logger
     * @param Tracer|null $tracer
     * @throws \ReflectionException
     */
    public function __construct(
        $maxMessageCount = null,
        ProcessManager $processManager = null,
        QueueInterface $pidKillerQueue = null,
        LoggerInterface $logger = null,
        Tracer $tracer = null
    ) {
        InsideConstruct::setConstructParams([
            'logger' => LoggerInterface::class,
            'tracer' => Tracer::class,
            'processManager' => ProcessManager::class,
        ]);

        if (strpos(php_uname(), 'Windows') === 0) {
            throw new RuntimeException('Pid killer does not work on Windows');
        }

        if ($maxMessageCount === null) {
            $maxMessageCount = self::DEF_MAX_MESSAGE_COUNT;
        }
        $this->processManager = $processManager ?? new ProcessManager();
        $this->maxMessageCount = $maxMessageCount;
    }

    public function getPidQueue(): QueueInterface
    {
        return $this->pidKillerQueue;
    }

    /**
     * Push new message to queue
     * record array:
     * [
     *  'id' - process pid,
     *  'delaySecond' - delay seconds to become message visible in queue
     * ]
     *
     * @param $record
     * @throws \rollun\utils\Json\Exception
     */
    public function create($record)
    {
        $span = $this->tracer->start('LinuxPidKiller::create', [
            new StringTag('pid', $record['pid']),
            new StringTag('delaySeconds', $record['delaySeconds'])
        ]);
        if (!isset($record['pid'])) {
            throw new InvalidArgumentException("Field 'pid' is missing");
        }

        if (!isset($record['delaySeconds'])) {
            throw new InvalidArgumentException("Field 'delaySeconds' is missing");
        }

        $lstart = $this->processManager->getPidStartTime($record['pid']);
        $id = $this->processManager->generateId($record['pid'], $lstart);
        $span->addTag(new StringTag('pid_start_time', $lstart));
        $span->addTag(new StringTag('pid_id', $id));

        if (!$lstart) {
            throw new RuntimeException("Process with pid {$record['pid']} does not exist");
        }

        $this->pidKillerQueue->addMessage(Message::createInstance([
            'id' => $id,
            QueueClient::KEY_DELAY_SECOND => $record['delaySeconds'],
            'Body' => $record['info'] ?? null,
            'TracerContext' => base64_encode(\rollun\utils\Json\Serializer::jsonSerialize($this->tracer->getContext()))
        ]));

        $this->logger->debug("PID-KILLER add pid {pid} to queue at {date}", [
            'date' => date('D d.m H:i:s'),
            'pid' => $record['pid'],
            'lstart' => $lstart,
        ]);

        $this->tracer->finish($span);
    }

    public function __invoke()
    {
        $this->logger->debug('PID-KILLER start working at {date}', [
            'date' => date('D d.m H:i:s'),
        ]);
        $span = $this->tracer->start('LinuxPidKiller::__invoke');

        $messageCount = 0;

        $pids = $this->processManager->ps();
        while ($messageCount < $this->maxMessageCount && $queueMessage = $this->pidKillerQueue->getMessage()) {
            $messageCount++;
            $message = $queueMessage->getData();

            $this->logger->debug('PID-KILLER get message from queue', [
                'message' => $message,
            ]);

            $id = array_search($message['id'], array_column($pids, 'id'), true);
            if ($id !== false) {
                [$pid, $lstart] = explode('.', $message['id']);

                $processKillSpan = $this->tracer->start('LinuxPidKiller::__kill', [
                    new StringTag('pid_id', $id),
                    new StringTag('pid', $pid),
                    new StringTag('pid_start_time', $lstart),
                ], $queueMessage->getTracerContext());

                $result = $this->processManager->kill($pid);

                $processKillSpan->addTag(new StringTag('kill_result', $result));

                if ($result) {
                    $processKillSpan->addTag(new ErrorTag());
                    $this->logger->warning('PID-KILLER failed kill process message from queue', [
                        'message' => $message,
                        'result' => $result,
                    ]);
                } else {
                    $this->pidKillerQueue->deleteMessage($queueMessage);
                    $this->logger->debug('PID-KILLER successfully kill process and delete message from queue', [
                        'message' => $message,
                    ]);
                }
                $this->tracer->finish($processKillSpan);
            } else {
                $this->pidKillerQueue->deleteMessage($queueMessage);
                $this->logger->debug('PID-KILLER process already ended and delete message from queue', [
                    'message' => $message,
                ]);
            }
        }

        $this->logger->debug('PID-KILLER finish working at {date}', [
            'date' => date('D d.m H:i:s'),
        ]);
        $this->tracer->finish($span);

    }


    /**
     * @return array
     */
    public function __sleep()
    {
        return ['pidKillerQueue', 'maxMessageCount', 'processManager'];
    }

    /**
     * @throws \ReflectionException
     */
    public function __wakeup()
    {
        InsideConstruct::initWakeup(['logger' => LoggerInterface::class,'tracer' => Tracer::class]);
    }
}
