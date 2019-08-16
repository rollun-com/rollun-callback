<?php
/**
 * @copyright Copyright © 2014 Rollun LC (http://rollun.com/)
 * @license LICENSE.md New BSD License
 */

namespace rollun\callback\PidKiller\Factory;

use Interop\Container\ContainerInterface;
use rollun\callback\PidKiller\Worker;
use rollun\callback\PidKiller\WorkerManager;
use Zend\ServiceManager\Factory\AbstractFactoryInterface;

/**
 * Config example:
 *
 *  [
 *      WorkerManagerAbstractFactory::class => [
 *          'requestedName1' => [
 *              'tableGateway' => 'tableGatewayServiceName',
 *              'process' => 'processServiceName',
 *              'workerManagerName' => 'very-important-worker-manager'
 *              'processCount' => 4
 *          ]
 *          'requestedName2' => [
 *              // ...
 *          ]
 *      ]
 *  ]
 *
 * Class WorkerAbstractFactory
 * @package rollun\callback\PidKiller\Factory
 */
class WorkerManagerAbstractFactory implements AbstractFactoryInterface
{
    const KEY_CLASS = 'class';

    const DEFAULT_CLASS = WorkerManager::class;

    const KEY_CACHE_STORAGE = 'cache_storage';

    const KEY_PROCESS = 'process';

    const KEY_WORKER_MANAGER_NAME = 'workerManagerName';

    const KEY_PROCESS_COUNT = 'processCount';

    public function __invoke(ContainerInterface $container, $requestedName, array $options = null)
    {
        $serviceConfig = $options ?? $container->get('config')[self::class][$requestedName];

        if (!isset($serviceConfig[self::KEY_CACHE_STORAGE])) {
            throw new \InvalidArgumentException("Invalid option '" . self::KEY_CACHE_STORAGE . "'");
        }

        if (!isset($serviceConfig[self::KEY_PROCESS])) {
            throw new \InvalidArgumentException("Invalid option '" . self::KEY_PROCESS . "'");
        }

        if (!isset($serviceConfig[self::KEY_WORKER_MANAGER_NAME])) {
            throw new \InvalidArgumentException("Invalid option '" . self::KEY_WORKER_MANAGER_NAME . "'");
        }

        if (!isset($serviceConfig[self::KEY_PROCESS_COUNT])) {
            throw new \InvalidArgumentException("Invalid option '" . self::KEY_PROCESS_COUNT . "'");
        }

        $cacheStorage = is_string($serviceConfig[self::KEY_CACHE_STORAGE]) ? $container->get($serviceConfig[self::KEY_CACHE_STORAGE]) : $serviceConfig[self::KEY_CACHE_STORAGE];

        $process = is_string($serviceConfig[self::KEY_PROCESS]) ? $container->get($serviceConfig[self::KEY_PROCESS]) : $serviceConfig[self::KEY_PROCESS];
        $workerManagerName = $serviceConfig[self::KEY_WORKER_MANAGER_NAME];
        $processCount = $serviceConfig[self::KEY_PROCESS_COUNT];
        $class = $serviceConfig[self::KEY_CLASS];

        return new $class($cacheStorage, $process, $workerManagerName, $processCount);
    }

    public function canCreate(ContainerInterface $container, $requestedName)
    {
        $class = $container->get('config')[self::class][$requestedName][self::KEY_CLASS] ?? null;

        if (is_a($class, self::DEFAULT_CLASS, true)) {
            return true;
        }

        return false;
    }
}
