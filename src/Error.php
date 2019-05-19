<?php

declare(strict_types=1);

namespace Amp\Elasticsearch;

class Error extends \Error
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
            $this->data = (array)json_decode($errorJson, true)['error'];
            $prettyErrorJson = json_encode(json_decode($errorJson, false), JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES);
            $message .= PHP_EOL . $prettyErrorJson;
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
