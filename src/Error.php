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
     * @param string|null $request
     */
    public function __construct(?string $errorJson, int $code, \Throwable $previous = null, readonly ?string $request = null)
    {
        $message = 'An error occurred. Response code: ' . $code . PHP_EOL . substr($errorJson ?: '', 0, 1024);
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
