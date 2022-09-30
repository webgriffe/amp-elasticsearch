<?php

declare(strict_types=1);

namespace Webgriffe\AmpElasticsearch;

use JsonException;

class Error extends \RuntimeException
{
    /**
     * @var array|null
     */
    private $data;

    /**
     * Error constructor.
     * @param string|null $errorJson
     * @param int $code
     */
    public function __construct(?string $errorJson, int $code)
    {
        $message = 'An error occurred. Response code: ' . $code;
        if ($errorJson) {
            try {
                $this->data = (array)json_decode($errorJson, true, 512, JSON_THROW_ON_ERROR);
                $prettyErrorJson = json_encode(
                    json_decode($errorJson, false, 512, JSON_THROW_ON_ERROR),
                    JSON_THROW_ON_ERROR | JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES
                );
                $message .= PHP_EOL . $prettyErrorJson;
            } catch (JsonException $jsonException) {
                // Do not fail because of JsonException
            }
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
