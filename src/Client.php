<?php

declare(strict_types=1);

namespace Webgriffe\AmpElasticsearch;

use Amp\Http\Client\HttpClient;
use Amp\Http\Client\HttpClientBuilder;
use Amp\Http\Client\Request;
use Amp\Http\Client\Response;
use function Amp\call;
use Amp\Promise;

class Client
{
    /**
     * @var string
     */
    private $baseUri;
    /**
     * @var HttpClient
     */
    private $httpClient;

    public function __construct(string $baseUri)
    {
        $this->httpClient = HttpClientBuilder::buildDefault();
        $this->baseUri = rtrim($baseUri, '/');
    }

    public function createIndex(string $index, array $body = null): Promise
    {
        $method = 'PUT';
        $uri = implode('/', [$this->baseUri, urlencode($index)]);
        return $this->doJsonRequest($method, $uri, $body);
    }

    public function existsIndex(string $index): Promise
    {
        $method = 'HEAD';
        $uri = implode('/', [$this->baseUri, urlencode($index)]);
        return $this->doJsonRequest($method, $uri);
    }

    public function getIndex(string $index): Promise
    {
        $method = 'GET';
        $uri = implode('/', [$this->baseUri, urlencode($index)]);
        return $this->doJsonRequest($method, $uri);
    }

    public function deleteIndex(string $index): Promise
    {
        $method = 'DELETE';
        $uri = implode('/', [$this->baseUri, urlencode($index)]);
        return $this->doJsonRequest($method, $uri);
    }

    public function statsIndex(string $index, string $metric = '_all', array $options = []): Promise
    {
        $method = 'GET';
        $uri = implode('/', [$this->baseUri, urlencode($index), '_stats', $metric]);
        if ($options) {
            $uri .= '?' . http_build_query($options);
        }
        return $this->doJsonRequest($method, $uri);
    }

    public function indexDocument(
        string $index,
        string $id,
        array $body,
        array $options = [],
        string $type = '_doc'
    ): Promise {
        $method = $id === '' ? 'POST' : 'PUT';
        $uri = implode('/', [$this->baseUri, urlencode($index), urlencode($type), urlencode($id)]);
        if ($options) {
            $uri .= '?' . http_build_query($options);
        }
        return $this->doRequest($this->createJsonRequest($method, $uri, json_encode($body)));
    }

    public function existsDocument(string $index, string $id, string $type = '_doc'): Promise
    {
        $method = 'HEAD';
        $uri = implode('/', [$this->baseUri, urlencode($index), urlencode($type), urlencode($id)]);
        return $this->doJsonRequest($method, $uri);
    }

    public function getDocument(string $index, string $id, array $options = [], string $type = '_doc'): Promise
    {
        $method = 'GET';
        $uri = implode('/', [$this->baseUri, urlencode($index), urlencode($type), urlencode($id)]);
        if ($options) {
            $uri .= '?' . http_build_query($options);
        }
        return $this->doJsonRequest($method, $uri);
    }

    public function deleteDocument(string $index, string $id, array $options = [], string $type = '_doc'): Promise
    {
        $method = 'DELETE';
        $uri = implode('/', [$this->baseUri, urlencode($index), urlencode($type), urlencode($id)]);
        if ($options) {
            $uri .= '?' . http_build_query($options);
        }
        return $this->doJsonRequest($method, $uri);
    }

    public function uriSearchOneIndex(string $index, string $query, array $options = []): Promise
    {
        return $this->uriSearch($index, $query, $options);
    }

    public function uriSearchManyIndices(array $indices, string $query, array $options = []): Promise
    {
        return $this->uriSearch(implode(',', $indices), $query, $options);
    }

    public function uriSearchAllIndices(string $query, array $options = []): Promise
    {
        return $this->uriSearch('_all', $query, $options);
    }

    public function catIndices(string $index = null, array $options = []): Promise
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
        return $this->doJsonRequest($method, $uri);
    }

    public function catHealth(array $options = []): Promise
    {
        $method = 'GET';
        $uri = implode('/', [$this->baseUri, '_cat', 'health']);
        if ($options) {
            $uri .= '?' . http_build_query($options);
        }
        return $this->doJsonRequest($method, $uri);
    }

    public function refresh(string $indexOrIndices = null, array $options = []): Promise
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
        return $this->doJsonRequest($method, $uri);
    }

    public function search(array $query, ?string $indexOrIndices = null, array $options = []): Promise
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
        return $this->doRequest($this->createJsonRequest($method, $uri, json_encode(['query' => $query])));
    }

    public function count(string $index, array $options = [], array $query = null): Promise
    {
        $method = 'GET';
        $uri = [$this->baseUri, $index];
        $uri[] = '_count';
        $uri = implode('/', $uri);
        if ($options) {
            $uri .= '?' . http_build_query($options);
        }
        if (null !== $query) {
            return $this->doRequest($this->createJsonRequest($method, $uri, json_encode(['query' => $query])));
        }
        return $this->doRequest($this->createJsonRequest($method, $uri));
    }

    public function bulk(array $body, string $index = null, array $options = []): Promise
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
            $this->createJsonRequest($method, $uri, implode(PHP_EOL, array_map('json_encode', $body)) . PHP_EOL)
        );
    }

    public function createOrUpdateAlias(string $target, string $alias, ?array $body = null): Promise
    {
        $method = 'PUT';
        $uri = implode('/', [$this->baseUri, urlencode($target), '_aliases', urlencode($alias)]);

        return $this->doJsonRequest($method, $uri, $body);
    }

    public function updateIndexSettings(string $target, array $body = null): Promise
    {
        $method = 'PUT';
        $uri = implode('/', [$this->baseUri, urlencode($target), '_settings']);

        return $this->doJsonRequest($method, $uri, $body);
    }

    private function createJsonRequest(string $method, string $uri, string $body = null): Request
    {
        $request = new Request($uri, $method);
        $request->setHeader('Content-Type', 'application/json');
        $request->setHeader('Accept', 'application/json');

        if ($body) {
            $request->setBody($body);
        }
        return $request;
    }

    private function doRequest(Request $request): Promise
    {
        return call(function () use ($request) {
            /** @var Response $response */
            $response = yield $this->httpClient->request($request);
            $body = yield $response->getBody()->buffer();
            $statusClass = (int) ($response->getStatus() / 100);
            if ($statusClass !== 2) {
                throw new Error($body, $response->getStatus());
            }
            if ($body === null) {
                return null;
            }
            return json_decode($body, true);
        });
    }

    private function uriSearch(string $indexOrIndicesOrAll, string $query, array $options): Promise
    {
        $method = 'GET';
        $uri = implode('/', [$this->baseUri, urlencode($indexOrIndicesOrAll), urlencode('_search')]);
        if (!empty($query)) {
            $options['q'] = $query;
        }
        if (!empty($options)) {
            $uri .= '?' . http_build_query($options);
        }
        return $this->doJsonRequest($method, $uri);
    }

    /**
     * @param string $method
     * @param string $uri
     * @return Promise
     *
     * @throws \JsonException
     */
    private function doJsonRequest(string $method, string $uri, array $body = null): Promise
    {
        $jsonBody = null;
        if ($body !== null) {
            $jsonBody = json_encode($body, JSON_THROW_ON_ERROR | JSON_FORCE_OBJECT);
        }
        return $this->doRequest($this->createJsonRequest($method, $uri, $jsonBody));
    }
}
