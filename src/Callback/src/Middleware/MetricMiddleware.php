<?php

namespace rollun\callback\Middleware;

use rollun\dic\InsideConstruct;
use Psr\Http\Message\ResponseInterface;
use Psr\Http\Message\ServerRequestInterface;
use Psr\Http\Server\MiddlewareInterface;
use Psr\Http\Server\RequestHandlerInterface;
use Psr\Log\LoggerInterface;

/**
 * Class MetricMiddleware
 *
 * @author    r.ratsun <r.ratsun.rollun@gmail.com>
 *
 * @copyright Copyright © 2014 Rollun LC (http://rollun.com/)
 * @license   LICENSE.md New BSD License
 */
class MetricMiddleware implements MiddlewareInterface
{
    /**
     * @var LoggerInterface
     */
    protected $logger;

    /**
     * MetricMiddleware constructor.
     *
     * @param LoggerInterface|null $logger
     *
     * @throws \ReflectionException
     */
    public function __construct(LoggerInterface $logger = null)
    {
        InsideConstruct::setConstructParams(['logger' => LoggerInterface::class]);
    }

    /**
     * Wake up
     */
    public function __wakeup()
    {
        InsideConstruct::initWakeup(['logger' => LoggerInterface::class]);
    }

    /**
     * @inheritDoc
     */
    public function process(ServerRequestInterface $request, RequestHandlerInterface $handler): ResponseInterface
    {
        if ($request->getUri()->getPath() == '/api/webhook/cron' && $request->getMethod() == 'GET') {
            $serviceName = getenv('SERVICE_NAME');
            if (!empty($serviceName)) {
                // prepare metric id
                $metricId = str_replace('-', '_', $serviceName) . '__webhook_cron_get__metric';

                // send metrics
                $this->logger->notice('METRICS', ['metricId' => $metricId, 'value' => 1]);
            }
        }

        return $handler->handle($request);
    }
}
