<?php

declare(strict_types=1);

namespace Webgriffe\AmpElasticsearch;

class Error extends \RuntimeException
{
    /**
     * @var array|null
     */
    private ?array $data = null;

    /**
     * Error constructor.
     * @param string|null $errorJson
     * @param int $code
     * @param \Throwable|null $previous
     */
    public function __construct(?string $errorJson, int $code, \Throwable $previous = null)
    {
        $message = 'An error occurred. Response code: ' . $code . PHP_EOL . $errorJson;
        if ($errorJson) {
            $this->data = json_decode($errorJson, true);
        }
        parent::__construct($message, $code, $previous);
    }

    /**
     * @return array|null
     */
    public function getData(): ?array
    {
        return $this->data;
    }
}
