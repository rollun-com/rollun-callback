<?php

namespace rollun\test\unit\Callback\PidKiller;

use Jaeger\Tracer\Tracer;
use PHPUnit\Framework\MockObject\MockObject;
use Psr\Log\LoggerInterface;
use Psr\SimpleCache\CacheInterface;
use rollun\callback\Callback\Interrupter\InterrupterInterface;
use rollun\callback\PidKiller\ProcessManager;
use rollun\callback\PidKiller\WorkerManager;
use PHPUnit\Framework\TestCase;
use rollun\callback\Promise\Interfaces\PayloadInterface;
use rollun\callback\Promise\SimplePayload;
use rollun\utils\Json\Serializer;
use Zend\Db\TableGateway\TableGateway;

class WorkerManagerTest extends TestCase
{

    /**
     * @throws \ReflectionException
     */
    public function test__invoke()
    {
        /**
         * @var $cache CacheInterface|MockObject
         */
        $cache = $this->getMockBuilder(CacheInterface::class)->getMock();
        $cache->method('get')->willReturn(Serializer::jsonSerialize([]));

        /**
         * @var $procesManager ProcessManager|MockObject
         */
        $procesManager = $this->getMockBuilder(ProcessManager::class)->getMock();

        /**
         * @var $logger LoggerInterface|MockObject
         */
        $logger = $this->getMockBuilder(LoggerInterface::class)->getMock();

        /**
         * @var $tracer Tracer|MockObject
         */
        $tracer = $this->getMockBuilder(Tracer::class)->disableOriginalConstructor()->getMock();

        $interrupter = new class() implements InterrupterInterface
        {
            private static $callCount = 0;

            /**
             * @param $value
             * @return mixed
             */
            public function __invoke($value = null): PayloadInterface
            {
                ++self::$callCount;
                return new SimplePayload(uniqid('test_', false), ['callCount' => self::$callCount]);
            }

            public function getCallCount() {
                return self::$callCount;
            }
        };

        $workerManager = new WorkerManager(
            $cache,
            $interrupter,
            'TestWorkerManager',
            5,
            $procesManager,
            1800,
            $logger,
            $tracer
        );

        $workerManager->__invoke();
        $this->assertEquals( 5, $interrupter->getCallCount());
    }
}
