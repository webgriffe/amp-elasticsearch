<?php

declare(strict_types=1);

namespace Webgriffe\AmpElasticsearch;

use Amp\ByteStream\BufferException;
use Amp\ByteStream\StreamException;
use Amp\Cancellation;
use Amp\Http\Client\Connection\UnprocessedRequestException;
use Amp\Http\Client\HttpClient;
use Amp\Http\Client\HttpClientBuilder;
use Amp\Http\Client\HttpException;
use Amp\Http\Client\Request;

class Client
{
    /**
     * @var string
     */
    private string $baseUri;
    /**
     * @var HttpClient
     */
    private HttpClient $httpClient;

    /**
     * @var float
     */
    private float $timeout = 10;

    /**
     * @param string $baseUri
     * @param HttpClient|null $httpClient
     */
    public function __construct(string $baseUri, HttpClient $httpClient = null)
    {
        $this->httpClient = $httpClient ?: HttpClientBuilder::buildDefault();
        $this->baseUri = rtrim($baseUri, '/');
    }

    /**
     * @param float $timeout
     * @return void
     */
    public function setTimeout(float $timeout): void
    {
        $this->timeout = $timeout;
    }

    /**
     * @param string $index
     * @param array|null $properties
     * @param array $options
     * @param Cancellation|null $cancellation
     * @return array|null
     */
    public function createIndex(string $index, ?array $properties = null, array $options = [], ?Cancellation $cancellation = null): ?array
    {
        $method = 'PUT';
        $uri = implode('/', [$this->baseUri, urlencode($index)]);
        if ($options) {
            $uri .= '?' . http_build_query($options);
        }
        return $this->doRequest($this->createJsonRequest($method, $uri, $properties ? json_encode($properties, JSON_UNESCAPED_UNICODE) : null), $cancellation);
    }

    /**
     * @param string $index
     * @param Cancellation|null $cancellation
     * @return bool
     * @throws Error
     */
    public function existsIndex(string $index, ?Cancellation $cancellation = null): bool
    {
        try {
            $this->doJsonRequest('HEAD', implode('/', [$this->baseUri, urlencode($index)]), $cancellation);
            return true;
        } catch (Error $e) {
            if ($e->getCode() == 404) {
                return false;
            } else {
                throw $e;
            }
        }
    }

    /**
     * @param string $name
     * @param Cancellation|null $cancellation
     * @return bool
     * @throws Error
     * @link https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-alias-exists.html
     */
    public function existsAlias(string $name, ?Cancellation $cancellation = null): bool
    {
        try {
            $this->doJsonRequest('HEAD', implode('/', [$this->baseUri, '_alias', urlencode($name)]), $cancellation);
            return true;
        } catch (Error $e) {
            if ($e->getCode() == 404) {
                return false;
            } else {
                throw $e;
            }
        }
    }

    /**
     * @param string $name
     * @param array $options
     * @param Cancellation|null $cancellation
     * @link https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-open-close.html
     */
    public function openIndex(string $name, array $options = [], ?Cancellation $cancellation = null): void
    {
        $uri = implode('/', [$this->baseUri, urlencode($name), '_open']);
        if ($options) {
            $uri .= '?' . http_build_query($options);
        }
        $this->doJsonRequest('POST', $uri, $cancellation);
    }

    /**
     * @param string $name
     * @param array $options
     * @param Cancellation|null $cancellation
     * @link https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-open-close.html
     */
    public function closeIndex(string $name, array $options = [], ?Cancellation $cancellation = null): void
    {
        $uri = implode('/', [$this->baseUri, urlencode($name), '_close']);
        if ($options) {
            $uri .= '?' . http_build_query($options);
        }
        $this->doJsonRequest('POST', $uri, $cancellation);
    }

    /**
     * @param array $actions
     * @param Cancellation|null $cancellation
     * @example $client->aliases([
     *   ['add'    => ['index' => 'logs-2013-07', 'alias' => 'current-month']],
     *   ['remove' => ['index' => 'logs-2013-06', 'alias' => 'current-month']],
     * ]);
     * @link https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-aliases.html
     */
    public function aliases(array $actions, ?Cancellation $cancellation = null): void
    {
        $uri = implode('/', [$this->baseUri, '_aliases']);
        $this->doRequest($this->createJsonRequest('POST', $uri, json_encode(['actions' => $actions], JSON_UNESCAPED_UNICODE)), $cancellation);
    }

    /**
     * @param string $index
     * @param Cancellation|null $cancellation
     * @return array
     * @link https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-get-alias.html
     */
    public function getIndexAliases(string $index, ?Cancellation $cancellation = null): array
    {
        try {
            return array_keys($this->doJsonRequest('GET', implode('/', [$this->baseUri, '_alias', urlencode($index)]), $cancellation));
        } catch (Error $e) {
            if ($e->getCode() == 404) {
                return [];
            } else {
                throw $e;
            }
        }
    }

    /**
     * @param string $index
     * @param Cancellation|null $cancellation
     * @return array|null
     * @throws Error
     */
    public function getIndex(string $index, ?Cancellation $cancellation = null): ?array
    {
        $method = 'GET';
        $uri = implode('/', [$this->baseUri, urlencode($index)]);
        return $this->doJsonRequest($method, $uri, $cancellation);
    }

    /**
     * @param string $index
     * @param Cancellation|null $cancellation
     * @return array|null
     * @throws Error
     */
    public function deleteIndex(string $index, ?Cancellation $cancellation = null): ?array
    {
        $method = 'DELETE';
        $uri = implode('/', [$this->baseUri, urlencode($index)]);
        return $this->doJsonRequest($method, $uri, $cancellation);
    }

    /**
     * @param string $index
     * @param string $metric
     * @param array $options
     * @param Cancellation|null $cancellation
     * @return array|null
     * @throws Error
     */
    public function statsIndex(string $index, string $metric = '_all', array $options = [], ?Cancellation $cancellation = null): ?array
    {
        $method = 'GET';
        $uri = implode('/', [$this->baseUri, urlencode($index), '_stats', $metric]);
        if ($options) {
            $uri .= '?' . http_build_query($options);
        }
        return $this->doJsonRequest($method, $uri, $cancellation);
    }

    /**
     * For only Elastic 7.+
     * @param string $index
     * @param array $properties
     * @param Cancellation|null $cancellation
     * @throws Error
     * @link https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-put-mapping.html
     */
    public function putMapping(string $index, array $properties, ?Cancellation $cancellation = null): void
    {
        $uri = implode('/', [$this->baseUri, urlencode($index), '_mapping']);
        $this->doRequest($this->createJsonRequest('PUT', $uri, json_encode(['properties' => $properties], JSON_UNESCAPED_UNICODE)), $cancellation);
    }

    /**
     * @param string $index
     * @param array $body
     * @param Cancellation|null $cancellation
     * @link https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-update-settings.html
     */
    public function putSettings(string $index, array $body, ?Cancellation $cancellation = null): void
    {
        $uri = implode('/', [$this->baseUri, urlencode($index), '_settings']);
        $this->doRequest($this->createJsonRequest('PUT', $uri, json_encode($body, JSON_UNESCAPED_UNICODE)), $cancellation);
    }

    /**
     * @param string $id
     * @param array $body
     * @param Cancellation|null $cancellation
     * @return array|null
     * @throws Error
     * @link https://www.elastic.co/guide/en/elasticsearch/reference/current/modules-scripting-using.html#script-stored-scripts
     */
    public function putScript(string $id, array $body, ?Cancellation $cancellation = null): ?array
    {
        $uri = implode('/', [$this->baseUri, '_scripts', urlencode($id)]);
        return $this->doRequest($this->createJsonRequest('PUT', $uri, json_encode($body, JSON_UNESCAPED_UNICODE)), $cancellation);
    }

    /**
     * @param string $index
     * @param string $id
     * @param array $body
     * @param array $options
     * @param string $type
     * @param Cancellation|null $cancellation
     * @return array|null
     * @throws Error
     */
    public function indexDocument(string $index, string $id, array $body, array $options = [], string $type = '_doc', ?Cancellation $cancellation = null): ?array
    {
        $method = $id === '' ? 'POST' : 'PUT';
        $uri = implode('/', [$this->baseUri, urlencode($index), urlencode($type), urlencode($id)]);
        if ($options) {
            $uri .= '?' . http_build_query($options);
        }
        return $this->doRequest($this->createJsonRequest($method, $uri, json_encode($body)), $cancellation);
    }

    /**
     * @param string $index
     * @param string $id
     * @param string $type
     * @param Cancellation|null $cancellation
     * @return bool
     * @throws Error
     */
    public function existsDocument(string $index, string $id, string $type = '_doc', ?Cancellation $cancellation = null): bool
    {
        try {
            $this->doJsonRequest('HEAD', implode('/', [$this->baseUri, urlencode($index), urlencode($type), urlencode($id)]), $cancellation);
            return true;
        } catch (Error $e) {
            if ($e->getCode() == 404) {
                return false;
            } else {
                throw $e;
            }
        }
    }

    /**
     * @param string $index
     * @param string $id
     * @param array $options
     * @param string $type
     * @param Cancellation|null $cancellation
     * @return array|null
     * @throws Error
     */
    public function getDocument(string $index, string $id, array $options = [], string $type = '_doc', ?Cancellation $cancellation = null): ?array
    {
        $method = 'GET';
        $uri = implode('/', [$this->baseUri, urlencode($index), urlencode($type), urlencode($id)]);
        if ($options) {
            $uri .= '?' . http_build_query($options);
        }
        return $this->doJsonRequest($method, $uri, $cancellation);
    }

    /**
     * @param string $index
     * @param string $id
     * @param array $options
     * @param string $type
     * @param Cancellation|null $cancellation
     * @return array|null
     * @throws Error
     */
    public function deleteDocument(string $index, string $id, array $options = [], string $type = '_doc', ?Cancellation $cancellation = null): ?array
    {
        $method = 'DELETE';
        $uri = implode('/', [$this->baseUri, urlencode($index), urlencode($type), urlencode($id)]);
        if ($options) {
            $uri .= '?' . http_build_query($options);
        }
        return $this->doJsonRequest($method, $uri, $cancellation);
    }

    /**
     * @param string $index
     * @param string $query
     * @param array $options
     * @param Cancellation|null $cancellation
     * @return array|null
     * @throws Error
     */
    public function uriSearchOneIndex(string $index, string $query, array $options = [], ?Cancellation $cancellation = null): ?array
    {
        return $this->uriSearch($index, $query, $options, $cancellation);
    }

    /**
     * @param array $indices
     * @param string $query
     * @param array $options
     * @param Cancellation|null $cancellation
     * @return array|null
     * @throws Error
     */
    public function uriSearchManyIndices(array $indices, string $query, array $options = [], ?Cancellation $cancellation = null): ?array
    {
        return $this->uriSearch(implode(',', $indices), $query, $options, $cancellation);
    }

    /**
     * @param string $query
     * @param array $options
     * @param Cancellation|null $cancellation
     * @return array|null
     * @throws Error
     */
    public function uriSearchAllIndices(string $query, array $options = [], ?Cancellation $cancellation = null): ?array
    {
        return $this->uriSearch('_all', $query, $options, $cancellation);
    }

    /**
     * @param string|null $index
     * @param array $options
     * @param Cancellation|null $cancellation
     * @return array|null
     * @throws Error
     */
    public function catIndices(string $index = null, array $options = [], ?Cancellation $cancellation = null): ?array
    {
        $method = 'GET';
        $uri = [$this->baseUri, '_cat', 'indices'];
        if ($index) {
            $uri[] = urlencode($index);
        }
        $uri = implode('/', $uri);
        if ($options) {
            $uri .= '?' . http_build_query($options);
        }
        return $this->doJsonRequest($method, $uri, $cancellation);
    }

    /**
     * @param array $options
     * @param Cancellation|null $cancellation
     * @return array|null
     * @throws Error
     */
    public function catHealth(array $options = [], ?Cancellation $cancellation = null): ?array
    {
        $method = 'GET';
        $uri = implode('/', [$this->baseUri, '_cat', 'health']);
        if ($options) {
            $uri .= '?' . http_build_query($options);
        }
        return $this->doJsonRequest($method, $uri, $cancellation);
    }

    /**
     * @param string|null $indexOrIndices
     * @param array $options
     * @param Cancellation|null $cancellation
     * @return array|null
     * @throws Error
     */
    public function refresh(?string $indexOrIndices = null, array $options = [], ?Cancellation $cancellation = null): ?array
    {
        $method = 'POST';
        $uri = [$this->baseUri];
        if ($indexOrIndices) {
            $uri[] = urlencode($indexOrIndices);
        }
        $uri[] = '_refresh';
        $uri = implode('/', $uri);
        if ($options) {
            $uri .= '?' . http_build_query($options);
        }
        return $this->doJsonRequest($method, $uri, $cancellation);
    }

    /**
     * @param array $body
     * @param array $options
     * @param Cancellation|null $cancellation
     * @return array|null
     * @link https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-reindex.html
     */
    public function reindex(array $body = [], array $options = [], ?Cancellation $cancellation = null): ?array
    {
        $uri = implode('/', [$this->baseUri, '_reindex']);
        if ($options) {
            $uri .= '?' . http_build_query($options);
        }
        return $this->doRequest($this->createJsonRequest('POST', $uri, json_encode($body)), $cancellation);
    }

    /**
     * @param string $task_id
     * @param array $options
     * @param Cancellation|null $cancellation
     * @return array|null
     * @link https://www.elastic.co/guide/en/elasticsearch/reference/current/tasks.html
     */
    public function getTask(string $task_id, array $options = [], ?Cancellation $cancellation = null): ?array
    {
        $uri = implode('/', [$this->baseUri, '_tasks', $task_id]);
        if ($options) {
            $uri .= '?' . http_build_query($options);
        }
        return $this->doJsonRequest('GET', $uri, $cancellation);
    }

    /**
     * @param array $query
     * @param string|null $indexOrIndices
     * @param array $options
     * @param Cancellation|null $cancellation
     * @return array|null
     * @throws Error
     */
    public function search(array $query, ?string $indexOrIndices = null, array $options = [], ?Cancellation $cancellation = null): ?array
    {
        $method = 'POST';
        $uri = [$this->baseUri];
        if ($indexOrIndices) {
            $uri[] = urlencode($indexOrIndices);
        }
        $uri[] = '_search';
        $uri = implode('/', $uri);
        if ($options) {
            $uri .= '?' . http_build_query($options);
        }
        return $this->doRequest($this->createJsonRequest($method, $uri, json_encode($query)), $cancellation);
    }

    /**
     * @param string $scroll_id
     * @param string $scroll
     * @param Cancellation|null $cancellation
     * @return array|null
     * @link https://www.elastic.co/guide/en/elasticsearch/reference/current/scroll-api.html
     */
    public function scroll(string $scroll_id, string $scroll = '1m', ?Cancellation $cancellation = null): ?array
    {
        $uri = [$this->baseUri, '_search', 'scroll'];
        return $this->doRequest($this->createJsonRequest('POST', implode('/', $uri), json_encode(['scroll_id' => $scroll_id, 'scroll' => $scroll])), $cancellation);
    }

    /**
     * @param string $index
     * @param array $options
     * @param array|null $query
     * @param Cancellation|null $cancellation
     * @return int
     * @throws Error
     */
    public function count(string $index, array $options = [], array $query = null, ?Cancellation $cancellation = null): int
    {
        $method = 'GET';
        $uri = [$this->baseUri, $index];
        $uri[] = '_count';
        $uri = implode('/', $uri);
        if ($options) {
            $uri .= '?' . http_build_query($options);
        }
        $result = $this->doRequest($this->createJsonRequest($method, $uri, $query ? json_encode($query) : null), $cancellation);
        return $result['count'];
    }

    /**
     * @param array|string $body
     * @param string|null $index
     * @param array $options
     * @param Cancellation|null $cancellation
     * @return array|null
     * @throws Error
     */
    public function bulk(array|string $body, string $index = null, array $options = [], ?Cancellation $cancellation = null): ?array
    {
        $method = 'POST';
        $uri = [$this->baseUri];
        if ($index) {
            $uri[] = urlencode($index);
        }
        $uri[] = '_bulk';
        $uri = implode('/', $uri);
        if ($options) {
            $uri .= '?' . http_build_query($options);
        }
        return $this->doRequest(
            $this->createJsonRequest($method, $uri, (is_array($body) ? implode(PHP_EOL, array_map('json_encode', $body)) : $body) . PHP_EOL), $cancellation
        );
    }

    /**
     * @param array $body
     * @param string|null $indexOrIndices
     * @param array $options
     * @param Cancellation|null $cancellation
     * @return array|null
     * @throws Error
     */
    public function updateByQuery(array $body, ?string $indexOrIndices = null, array $options = [], ?Cancellation $cancellation = null): ?array
    {
        $method = 'POST';
        $uri = [$this->baseUri];
        if ($indexOrIndices) {
            $uri[] = urlencode($indexOrIndices);
        }
        $uri[] = '_update_by_query';
        $uri = implode('/', $uri);
        if ($options) {
            $uri .= '?' . http_build_query($options);
        }
        return $this->doRequest($this->createJsonRequest($method, $uri, json_encode($body)), $cancellation);
    }

    /**
     * @param string $method
     * @param string $uri
     * @param string|null $body
     * @return Request
     */
    private function createJsonRequest(string $method, string $uri, string $body = null): Request
    {
        $request = new Request($uri, $method);
        $request->setHeader('Content-Type', 'application/json');
        $request->setHeader('Accept', 'application/json');
        $request->setBodySizeLimit(15000000);
        $request->setHeaderSizeLimit(32768);
        $request->setInactivityTimeout($this->timeout);
        $request->setTransferTimeout($this->timeout);
        $request->setTcpConnectTimeout($this->timeout);
        $request->setTlsHandshakeTimeout($this->timeout);

        if ($body) {
            $request->setBody($body);
        }
        return $request;
    }

    /**
     * @param Request $request
     * @param Cancellation|null $cancellation
     * @return array|null
     * @throws Error
     */
    private function doRequest(Request $request, ?Cancellation $cancellation = null): ?array
    {
        try {
            $response = $this->httpClient->request($request, $cancellation);
            $body = $response->getBody()->buffer();
            $statusClass = (int)($response->getStatus() / 100);
            if ($statusClass !== 2) {
                throw new Error($body, $response->getStatus(), null, $request->getBody()->getContent()->read());
            }
            return json_decode($body, true);
        } catch (UnprocessedRequestException $e) {
            throw new Error(null, 500, $e->getPrevious());
        } catch (HttpException|StreamException $e) {
            throw new Error(null, 500, $e);
        }
    }

    /**
     * @param string $indexOrIndicesOrAll
     * @param string $query
     * @param array $options
     * @param Cancellation|null $cancellation
     * @return array|null
     * @throws Error
     */
    private function uriSearch(string $indexOrIndicesOrAll, string $query, array $options, ?Cancellation $cancellation = null): ?array
    {
        $method = 'GET';
        $uri = implode('/', [$this->baseUri, urlencode($indexOrIndicesOrAll), urlencode('_search')]);
        if (!empty($query)) {
            $options['q'] = $query;
        }
        if (!empty($options)) {
            $uri .= '?' . http_build_query($options);
        }
        return $this->doJsonRequest($method, $uri, $cancellation);
    }

    /**
     * @param string $method
     * @param string $uri
     * @param Cancellation|null $cancellation
     * @return array|null
     * @throws Error
     */
    private function doJsonRequest(string $method, string $uri, ?Cancellation $cancellation = null): ?array
    {
        return $this->doRequest($this->createJsonRequest($method, $uri), $cancellation);
    }
}
