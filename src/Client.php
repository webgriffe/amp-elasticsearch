<?php

declare(strict_types=1);

namespace Amp\Elasticsearch;

use Amp\Artax\Client as HttpClient;
use Amp\Artax\DefaultClient;
use Amp\Artax\Request;
use Amp\Artax\Response;
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

    public function __construct(string $baseUri, HttpClient $httpClient = null)
    {
        $this->httpClient = new DefaultClient();
        if ($httpClient) {
            $this->httpClient = $httpClient;
        }
        $this->baseUri = rtrim($baseUri, '/');
    }

    public function createIndex(string $index): Promise
    {
        $method = 'PUT';
        $uri = implode('/', [$this->baseUri, urlencode($index)]);
        return $this->doRequest($this->createJsonRequest($method, $uri));
    }

    public function existsIndex(string $index): Promise
    {
        $method = 'HEAD';
        $uri = implode('/', [$this->baseUri, urlencode($index)]);
        return $this->doRequest($this->createJsonRequest($method, $uri));
    }

    public function getIndex(string $index): Promise
    {
        $method = 'GET';
        $uri = implode('/', [$this->baseUri, urlencode($index)]);
        return $this->doRequest($this->createJsonRequest($method, $uri));
    }

    public function deleteIndex(string $index): Promise
    {
        $method = 'DELETE';
        $uri = implode('/', [$this->baseUri, urlencode($index)]);
        return $this->doRequest($this->createJsonRequest($method, $uri));
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
        return $this->doRequest($this->createJsonRequest($method, $uri, $body));
    }

    public function existsDocument(string $index, string $id, string $type = '_doc'): Promise
    {
        $method = 'HEAD';
        $uri = implode('/', [$this->baseUri, urlencode($index), urlencode($type), urlencode($id)]);
        return $this->doRequest($this->createJsonRequest($method, $uri));
    }

    public function getDocument(string $index, string $id, array $options = [], string $type = '_doc'): Promise
    {
        $method = 'GET';
        $uri = implode('/', [$this->baseUri, urlencode($index), urlencode($type), urlencode($id)]);
        if ($options) {
            $uri .= '?' . http_build_query($options);
        }
        return $this->doRequest($this->createJsonRequest($method, $uri));
    }

    public function deleteDocument(string $index, string $id, array $options = [], string $type = '_doc'): Promise
    {
        $method = 'DELETE';
        $uri = implode('/', [$this->baseUri, urlencode($index), urlencode($type), urlencode($id)]);
        if ($options) {
            $uri .= '?' . http_build_query($options);
        }
        return $this->doRequest($this->createJsonRequest($method, $uri));
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

    private function createJsonRequest(string $method, string $uri, array $body = null): Request
    {
        $request = (new Request($uri, $method))
            ->withHeader('Content-Type', 'application/json');
        if ($body) {
            $request = $request->withBody(json_encode($body));
        }
        return $request;
    }

    private function doRequest(Request $request): Promise
    {
        return call(function () use ($request) {
            /** @var Response $response */
            $response = yield $this->httpClient->request($request);
            $body = yield $response->getBody();
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
        $options['q'] = $query;
        $uri .= '?' . http_build_query($options);
        return $this->doRequest($this->createJsonRequest($method, $uri));
    }
}
