# Amp ElasticSearch Client

`webgriffe/amp-elasticsearch` is a non-blocking ElasticSearch client for use with the [`amp`](https://github.com/amphp/amp)
concurrency framework.

[![Build Status](https://github.com/webgriffe/amp-elasticsearch/workflows/Build/badge.svg)](https://github.com/webgriffe/amp-elasticsearch/actions)

**Required PHP Version**

- PHP 7.4
- PHP 8.0

**Installation**

```bash
composer require webgriffe/amp-elasticsearch
```

**Usage**

Just create a client instance and call its public methods which returns promises:

```php
Loop::run(function () {
  $client = new Webgriffe\AmpElasticsearch\Client('http://my.elasticsearch.test:9200');
  yield $this->client->createIndex('myindex');
  $response = yield $this->client->indexDocument('myindex', '', ['testField' => 'abc']);
  echo $response['result']; // 'created'
});
```

See other usage examples in the [`tests/Integration/ClientTest.php`](./tests/Integration/ClientTest.php).

All client methods return an array representation of the ElasticSearch REST API responses in case of sucess or an `Webgriffe\AmpElasticsearch\Error` in case of error.

## Security

If you discover any security related issues, please email [`support@webgriffe.com`](mailto:support@webgriffe.com) instead of using the issue tracker.

## License

The MIT License (MIT). Please see [`LICENSE`](./LICENSE) for more information.
