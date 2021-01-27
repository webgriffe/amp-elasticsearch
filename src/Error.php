<?php

declare(strict_types=1);

namespace Webgriffe\AmpElasticsearch;

class Error extends \RuntimeException
{
    /**
     * @var array|null
     */
    private $data;

    /**
     * Error constructor.
     * @param string $errorJson
     * @param int $code
     */
    public function __construct(?string $errorJson, int $code)
    {
        $message = 'An error occurred. Response code: ' . $code;
        if ($errorJson) {
            $this->data = json_decode($errorJson, true);
        }
        parent::__construct($message, $code);
    }

    /**
     * @return array|null
     */
    public function getData(): ?array
    {
        return $this->data;
    }
}
