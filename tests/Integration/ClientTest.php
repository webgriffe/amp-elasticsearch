<?php

declare(strict_types=1);

namespace Webgriffe\AmpElasticsearch\Tests\Integration;

use Webgriffe\AmpElasticsearch\Client;
use PHPUnit\Framework\TestCase;
use function Amp\delay;

class ClientTest extends TestCase
{
    const TEST_INDEX = 'test_index';
    const DEFAULT_ES_URL = 'http://127.0.0.1:9200';

    private Client $client;

    protected function setUp(): void
    {
        $esUrl = getenv('ES_URL') ?: self::DEFAULT_ES_URL;
        $this->client = new Client($esUrl);
        foreach ([self::TEST_INDEX, 'test_another_index', 'test_an_index'] as $index) {
            try {
                $this->client->deleteIndex($index);
            } catch (\Throwable $e) {
            }
        }
    }

    public function testCreateIndex(): void
    {
        $response = $this->client->createIndex(self::TEST_INDEX);
        $this->assertIsArray($response);
        $this->assertTrue($response['acknowledged']);
        $this->assertEquals(self::TEST_INDEX, $response['index']);
    }

    public function testIndicesExistsShouldThrow404ErrorIfIndexDoesNotExists(): void
    {
        $this->assertFalse($this->client->existsIndex(self::TEST_INDEX));
    }

    public function testIndicesExistsShouldNotThrowAnErrorIfIndexExists(): void
    {
        $this->client->createIndex(self::TEST_INDEX);
        $this->assertTrue($this->client->existsIndex(self::TEST_INDEX));
    }

    public function testDocumentsIndex(): void
    {
        $response = $this->client->indexDocument(self::TEST_INDEX, 'my_id', ['testField' => 'abc']);
        $this->assertIsArray($response);
        $this->assertEquals(self::TEST_INDEX, $response['_index']);
    }

    public function testDocumentsIndexWithAutomaticIdCreation(): void
    {
        $response = $this->client->indexDocument(self::TEST_INDEX, '', ['testField' => 'abc']);
        $this->assertIsArray($response);
        $this->assertEquals(self::TEST_INDEX, $response['_index']);
        $this->assertEquals('created', $response['result']);
    }

    public function testDocumentsExistsShouldThrowA404ErrorIfDocumentDoesNotExists(): void
    {
        $this->client->createIndex(self::TEST_INDEX);
        $this->assertFalse($this->client->existsDocument(self::TEST_INDEX, 'not-existent-doc'));
    }

    public function testDocumentsExistsShouldNotThrowAnErrorIfDocumentExists(): void
    {
        $this->client->indexDocument(self::TEST_INDEX, 'my_id', ['testField' => 'abc']);
        $this->assertTrue($this->client->existsDocument(self::TEST_INDEX, 'my_id'));
    }

    public function testDocumentsGet(): void
    {
        $this->client->indexDocument(self::TEST_INDEX, 'my_id', ['testField' => 'abc']);
        $response = $this->client->getDocument(self::TEST_INDEX, 'my_id');
        $this->assertIsArray($response);
        $this->assertTrue($response['found']);
        $this->assertEquals('my_id', $response['_id']);
        $this->assertEquals('abc', $response['_source']['testField']);
    }

    public function testDocumentsGetWithOptions(): void
    {
        $this->client->indexDocument(self::TEST_INDEX, 'my_id', ['testField' => 'abc']);
        $response = $this->client->getDocument(self::TEST_INDEX, 'my_id', ['_source' => 'false']);
        $this->assertIsArray($response);
        $this->assertTrue($response['found']);
        $this->assertArrayNotHasKey('_source', $response);
    }

    public function testDocumentsGetWithOnlySource(): void
    {
        $this->client->indexDocument(self::TEST_INDEX, 'my_id', ['testField' => 'abc']);
        $response = $this->client->getDocument(self::TEST_INDEX, 'my_id', []);
        $this->assertIsArray($response);
        $this->assertEquals('abc', $response['_source']['testField']);
    }

    public function testDocumentsDelete(): void
    {
        $this->client->indexDocument(self::TEST_INDEX, 'my_id', ['testField' => 'abc']);
        $response = $this->client->deleteDocument(self::TEST_INDEX, 'my_id');
        $this->assertIsArray($response);
        $this->assertEquals('deleted', $response['result']);
    }

    public function testUriSearchOneIndex(): void
    {
        $this->client->indexDocument(self::TEST_INDEX, 'my_id', ['testField' => 'abc'], ['refresh' => 'true']);
        $response = $this->client->uriSearchOneIndex(self::TEST_INDEX, 'testField:abc');
        $this->assertIsArray($response);
        $this->assertCount(1, $response['hits']['hits']);
    }

    public function testUriSearchAllIndices(): void
    {
        $this->client->indexDocument(self::TEST_INDEX, 'my_id', ['testField' => 'abc'], ['refresh' => 'true']);
        $response = $this->client->uriSearchAllIndices('testField:abc');
        $this->assertIsArray($response);
        $this->assertCount(1, $response['hits']['hits']);
    }

    public function testUriSearchManyIndices(): void
    {
        $this->client->indexDocument(self::TEST_INDEX, 'my_id', ['testField' => 'abc'], ['refresh' => 'true']);
        $response = $this->client->uriSearchManyIndices([self::TEST_INDEX], 'testField:abc');
        $this->assertIsArray($response);
        $this->assertCount(1, $response['hits']['hits']);
    }

    public function testStatsIndexWithAllMetric(): void
    {
        $this->client->indexDocument(self::TEST_INDEX, 'my_id', ['testField' => 'abc'], ['refresh' => 'true']);
        $response = $this->client->statsIndex(self::TEST_INDEX);
        $this->assertEquals(1, $response['indices'][self::TEST_INDEX]['total']['indexing']['index_total']);
    }

    public function testStatsIndexWithDocsMetric(): void
    {
        $this->client->indexDocument(self::TEST_INDEX, 'my_id', ['testField' => 'abc'], ['refresh' => 'true']);
        $response = $this->client->statsIndex(self::TEST_INDEX, 'docs');
        $this->assertArrayNotHasKey('indexing', $response['indices'][self::TEST_INDEX]['total']);
        $this->assertEquals(1, $response['indices'][self::TEST_INDEX]['total']['docs']['count']);
    }

    public function testCatIndices(): void
    {
        $this->client->indexDocument(self::TEST_INDEX, 'my_id', ['testField' => 'abc'], ['refresh' => 'true']);
        $response = $this->client->catIndices('test_*');
        $this->assertCount(1, $response);
        $this->assertEquals(self::TEST_INDEX, $response[0]['index']);
    }

    public function testCatIndicesWithoutIndices(): void
    {
        $response = $this->client->catIndices('test_*');
        $this->assertCount(0, $response);
    }

    public function testCatIndicesWithSpecificIndex(): void
    {
        $this->client->indexDocument(self::TEST_INDEX, 'my_id', ['testField' => 'abc'], ['refresh' => 'true']);
        $this->client->indexDocument('test_another_index', 'my_id', ['testField' => 'abc'], ['refresh' => 'true']);
        $response = $this->client->catIndices(self::TEST_INDEX);
        $this->assertCount(1, $response);
        $this->assertEquals(self::TEST_INDEX, $response[0]['index']);
    }

    public function testCatHealth(): void
    {
        $response = $this->client->catHealth();
        $this->assertCount(1, $response);
        $this->assertArrayHasKey('status', $response[0]);
    }

    public function testRefreshOneIndex(): void
    {
        $this->client->createIndex(self::TEST_INDEX);
        $response = $this->client->refresh(self::TEST_INDEX);
        $this->assertCount(1, $response);
    }

    public function testRefreshManyIndices(): void
    {
        $this->client->createIndex('test_an_index');
        $this->client->createIndex('test_another_index');
        $response = $this->client->refresh('test_an_index,test_another_index');
        $this->assertCount(1, $response);
    }

    public function testRefreshAllIndices(): void
    {
        $this->client->createIndex(self::TEST_INDEX);
        $response = $this->client->refresh();
        $this->assertCount(1, $response);
    }

    public function testSearch(): void
    {
        $this->client->createIndex(self::TEST_INDEX);
        $this->client->indexDocument(self::TEST_INDEX, 'document-id', ['uuid' => 'this-is-a-uuid', 'payload' => []], ['refresh' => 'true']);
        $query = [
            'query' => [
                'term' => [
                    'uuid.keyword' => [
                        'value' => 'this-is-a-uuid'
                    ]
                ]
            ]
        ];
        $response = $this->client->search($query);
        $this->assertIsArray($response);
        $this->assertCount(1, $response['hits']['hits']);
    }

    public function testUpdateByQuery(): void
    {
        $this->client->createIndex(self::TEST_INDEX);
        $this->client->indexDocument(self::TEST_INDEX, 'document-id', ['uuid' => 'this-is-a-uuid', 'payload' => '1'], ['refresh' => 'true']);
        $query = [
            'query' => [
                'term' => [
                    'uuid.keyword' => [
                        'value' => 'this-is-a-uuid'
                    ]
                ]
            ]
        ];
        $response = $this->client->search($query);
        $this->assertIsArray($response);
        $this->assertCount(1, $response['hits']['hits']);
        $this->assertEquals('1', $response['hits']['hits'][0]['_source']['payload']);

        $this->client->updateByQuery(array_merge($query, ['script' => [
            'source' => 'ctx._source[\'payload\'] = \'2\'',
            'lang' => 'painless',
        ]]), self::TEST_INDEX, ['conflicts' => 'proceed']);
        delay(1);
        $response = $this->client->search($query);
        $this->assertEquals('2', $response['hits']['hits'][0]['_source']['payload']);
    }

    public function testCount(): void
    {
        $this->client->createIndex(self::TEST_INDEX);
        $this->client->indexDocument(self::TEST_INDEX, '', ['payload' => []], ['refresh' => 'true']);
        $this->client->indexDocument(self::TEST_INDEX, '', ['payload' => []], ['refresh' => 'true']);

        $response = $this->client->count(self::TEST_INDEX);

        $this->assertIsArray($response);
        $this->assertEquals(2, $response['count']);
    }

    public function testCountWithQuery(): void
    {
        $this->client->createIndex(self::TEST_INDEX);
        $this->client->indexDocument(self::TEST_INDEX, '', ['user' => 'kimchy'], ['refresh' => 'true']);
        $this->client->indexDocument(self::TEST_INDEX, '', ['user' => 'foo'], ['refresh' => 'true']);

        $response = $this->client->count(self::TEST_INDEX, [], ['query' => ['term' => ['user' => 'kimchy']]]);

        $this->assertIsArray($response);
        $this->assertEquals(1, $response['count']);
    }

    public function testBulkIndex(): void
    {
        $this->client->createIndex(self::TEST_INDEX);
        $body = [];
        $responses = [];
        for ($i = 1; $i <= 1234; $i++) {
            $body[] = ['index' => ['_id' => $i, '_type' => '_doc']];
            $body[] = ['test' => 'bulk', 'my_field' => 'my_value_' . $i];

            // Every 100 documents stop and send the bulk request
            if ($i % 100 === 0) {
                $responses = $this->client->bulk($body, self::TEST_INDEX);
                $body = [];
                unset($responses);
            }
        }
        if (!empty($body)) {
            $responses = $this->client->bulk($body, self::TEST_INDEX);
        }

        $this->assertIsArray($responses);
        $this->assertCount(34, $responses['items']);
    }

    public function testAliasExists(): void
    {
        $this->assertFalse($this->client->existsAlias(self::TEST_INDEX));

        $this->client->createIndex(self::TEST_INDEX);
        $this->assertFalse($this->client->existsAlias(self::TEST_INDEX));
    }

    public function testCreateAlias(): void
    {
        $this->assertFalse($this->client->existsAlias(self::TEST_INDEX));

        $this->client->createIndex('test_another_index');
        $this->client->aliases([
            ['add' => ['index' => 'test_another_index', 'alias' => self::TEST_INDEX]],
        ]);
        $this->assertTrue($this->client->existsAlias(self::TEST_INDEX));
    }

    public function testAliases(): void
    {
        $this->client->createIndex('test_another_index');
        $this->client->createIndex('test_an_index');
        $this->client->aliases([
            ['add' => ['index' => 'test_another_index', 'alias' => self::TEST_INDEX]],
            ['add' => ['index' => 'test_an_index', 'alias' => self::TEST_INDEX]],
        ]);

        $this->assertEquals(['test_an_index', 'test_another_index'], $this->client->getIndexAliases(self::TEST_INDEX));
    }

    public function testReindex(): void
    {
        $this->client->createIndex('test_another_index');
        $this->client->createIndex(self::TEST_INDEX);
        $this->client->indexDocument(self::TEST_INDEX, 'my_id', ['testField' => 'abc'], ['refresh' => '']);
        $this->assertTrue($this->client->existsDocument(self::TEST_INDEX, 'my_id'));
        $this->assertFalse($this->client->existsDocument('test_another_index', 'my_id'));

        $result = $this->client->reindex([
            'conflicts' => 'proceed',
            'source' => [
                'index' => self::TEST_INDEX,
            ],
            'dest' => [
                'index' => 'test_another_index',
                'op_type' => 'create',
            ],
        ], ['refresh' => '']);
        $this->assertNotNull($result);
        $this->assertTrue($this->client->existsDocument(self::TEST_INDEX, 'my_id'));
        $this->assertTrue($this->client->existsDocument('test_another_index', 'my_id'));
    }
}
