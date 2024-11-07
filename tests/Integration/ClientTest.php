<?php

declare(strict_types=1);

namespace Webgriffe\AmpElasticsearch\Tests\Integration;

use Webgriffe\AmpElasticsearch\Client;
use Webgriffe\AmpElasticsearch\Error;
use Amp\Promise;
use PHPUnit\Framework\TestCase;

class ClientTest extends TestCase
{
    const TEST_INDEX = 'test_index';
    const DEFAULT_ES_URL = 'http://127.0.0.1:9200';

    /**
     * @var Client
     */
    private $client;

    protected function setUp(): void
    {
        $esUrl = getenv('ES_URL') ?: self::DEFAULT_ES_URL;
        $this->client = new Client($esUrl);
        $indices = Promise\wait($this->client->catIndices());
        foreach ($indices as $index) {
            Promise\wait($this->client->deleteIndex($index['index']));
        }
    }

    public function testCreateIndex(): void
    {
        $response = Promise\wait($this->client->createIndex(self::TEST_INDEX));
        $this->assertIsArray($response);
        $this->assertTrue($response['acknowledged']);
        $this->assertEquals(self::TEST_INDEX, $response['index']);
    }

    public function testCreateIndexWithExplicitMappingSettingsAndAliases(): void
    {
        $response = Promise\wait(
            $this->client->createIndex(
                self::TEST_INDEX,
                [
                    'mappings' => ['properties' => ['testField' => ['type' => 'text']]],
                    'settings' => ['index' => ['mapping' => ['total_fields' => ['limit' => 2000]]]],
                    'aliases' => ['alias1' => [], 'alias2' => ['filter' => ['term' => ['user' => 'kimchy']]]]
                ]
            )
        );
        $this->assertIsArray($response);
        $this->assertTrue($response['acknowledged']);
        $this->assertEquals(self::TEST_INDEX, $response['index']);
        $response = Promise\wait($this->client->getIndex(self::TEST_INDEX));
        $this->assertEquals('text', $response[self::TEST_INDEX]['mappings']['properties']['testField']['type']);
        $this->assertEquals(2000, $response[self::TEST_INDEX]['settings']['index']['mapping']['total_fields']['limit']);
        $this->assertCount(2, $response[self::TEST_INDEX]['aliases']);
        $this->assertEquals('kimchy', $response[self::TEST_INDEX]['aliases']['alias2']['filter']['term']['user']);
    }

    public function testIndicesExistsShouldThrow404ErrorIfIndexDoesNotExists(): void
    {
        $this->expectException(Error::class);
        $this->expectExceptionCode(404);
        Promise\wait($this->client->existsIndex(self::TEST_INDEX));
    }

    public function testIndicesExistsShouldNotThrowAnErrorIfIndexExists(): void
    {
        Promise\wait($this->client->createIndex(self::TEST_INDEX));
        $response = Promise\wait($this->client->existsIndex(self::TEST_INDEX));
        $this->assertNull($response);
    }

    public function testDocumentsIndex(): void
    {
        $response = Promise\wait($this->client->indexDocument(self::TEST_INDEX, 'my_id', ['testField' => 'abc']));
        $this->assertIsArray($response);
        $this->assertEquals(self::TEST_INDEX, $response['_index']);
    }

    public function testDocumentsIndexWithAutomaticIdCreation(): void
    {
        $response = Promise\wait($this->client->indexDocument(self::TEST_INDEX, '', ['testField' => 'abc']));
        $this->assertIsArray($response);
        $this->assertEquals(self::TEST_INDEX, $response['_index']);
        $this->assertEquals('created', $response['result']);
    }

    public function testDocumentsExistsShouldThrowA404ErrorIfDocumentDoesNotExists(): void
    {
        Promise\wait($this->client->createIndex(self::TEST_INDEX));
        $this->expectException(Error::class);
        $this->expectExceptionCode(404);
        Promise\wait($this->client->existsDocument(self::TEST_INDEX, 'not-existent-doc'));
    }

    public function testDocumentsExistsShouldNotThrowAnErrorIfDocumentExists(): void
    {
        Promise\wait($this->client->indexDocument(self::TEST_INDEX, 'my_id', ['testField' => 'abc']));
        $response = Promise\wait($this->client->existsDocument(self::TEST_INDEX, 'my_id'));
        $this->assertNull($response);
    }

    public function testDocumentsGet(): void
    {
        Promise\wait($this->client->indexDocument(self::TEST_INDEX, 'my_id', ['testField' => 'abc']));
        $response = Promise\wait($this->client->getDocument(self::TEST_INDEX, 'my_id'));
        $this->assertIsArray($response);
        $this->assertTrue($response['found']);
        $this->assertEquals('my_id', $response['_id']);
        $this->assertEquals('abc', $response['_source']['testField']);
    }

    public function testDocumentsGetWithOptions(): void
    {
        Promise\wait($this->client->indexDocument(self::TEST_INDEX, 'my_id', ['testField' => 'abc']));
        $response = Promise\wait($this->client->getDocument(self::TEST_INDEX, 'my_id', ['_source' => 'false']));
        $this->assertIsArray($response);
        $this->assertTrue($response['found']);
        $this->assertArrayNotHasKey('_source', $response);
    }

    public function testDocumentsGetWithOnlySource(): void
    {
        Promise\wait($this->client->indexDocument(self::TEST_INDEX, 'my_id', ['testField' => 'abc']));
        $response = Promise\wait($this->client->getDocument(self::TEST_INDEX, 'my_id', [], '_source'));
        $this->assertIsArray($response);
        $this->assertEquals('abc', $response['testField']);
    }

    public function testDocumentsDelete(): void
    {
        Promise\wait($this->client->indexDocument(self::TEST_INDEX, 'my_id', ['testField' => 'abc']));
        $response = Promise\wait($this->client->deleteDocument(self::TEST_INDEX, 'my_id'));
        $this->assertIsArray($response);
        $this->assertEquals('deleted', $response['result']);
    }

    public function testUriSearchOneIndex(): void
    {
        Promise\wait(
            $this->client->indexDocument(self::TEST_INDEX, 'my_id', ['testField' => 'abc'], ['refresh' => 'true'])
        );
        $response = Promise\wait($this->client->uriSearchOneIndex(self::TEST_INDEX, 'testField:abc'));
        $this->assertIsArray($response);
        $this->assertCount(1, $response['hits']['hits']);
    }

    public function testUriSearchAllIndices(): void
    {
        Promise\wait(
            $this->client->indexDocument(self::TEST_INDEX, 'my_id', ['testField' => 'abc'], ['refresh' => 'true'])
        );
        $response = Promise\wait($this->client->uriSearchAllIndices('testField:abc'));
        $this->assertIsArray($response);
        $this->assertCount(1, $response['hits']['hits']);
    }

    public function testUriSearchManyIndices(): void
    {
        Promise\wait(
            $this->client->indexDocument(self::TEST_INDEX, 'my_id', ['testField' => 'abc'], ['refresh' => 'true'])
        );
        $response = Promise\wait($this->client->uriSearchManyIndices([self::TEST_INDEX], 'testField:abc'));
        $this->assertIsArray($response);
        $this->assertCount(1, $response['hits']['hits']);
    }

    public function testStatsIndexWithAllMetric(): void
    {
        Promise\wait(
            $this->client->indexDocument(self::TEST_INDEX, 'my_id', ['testField' => 'abc'], ['refresh' => 'true'])
        );
        $response = Promise\wait($this->client->statsIndex(self::TEST_INDEX));
        $this->assertEquals(1, $response['indices'][self::TEST_INDEX]['total']['indexing']['index_total']);
    }

    public function testStatsIndexWithDocsMetric(): void
    {
        Promise\wait(
            $this->client->indexDocument(self::TEST_INDEX, 'my_id', ['testField' => 'abc'], ['refresh' => 'true'])
        );
        $response = Promise\wait($this->client->statsIndex(self::TEST_INDEX, 'docs'));
        $this->assertArrayNotHasKey('indexing', $response['indices'][self::TEST_INDEX]['total']);
        $this->assertEquals(1, $response['indices'][self::TEST_INDEX]['total']['docs']['count']);
    }

    public function testCatIndices(): void
    {
        Promise\wait(
            $this->client->indexDocument(self::TEST_INDEX, 'my_id', ['testField' => 'abc'], ['refresh' => 'true'])
        );
        $response = Promise\wait($this->client->catIndices());
        $this->assertCount(1, $response);
        $this->assertEquals(self::TEST_INDEX, $response[0]['index']);
    }

    public function testCatIndicesWithoutIndices(): void
    {
        $response = Promise\wait($this->client->catIndices());
        $this->assertCount(0, $response);
    }

    public function testCatIndicesWithSpecificIndex(): void
    {
        Promise\wait(
            $this->client->indexDocument(self::TEST_INDEX, 'my_id', ['testField' => 'abc'], ['refresh' => 'true'])
        );
        Promise\wait(
            $this->client->indexDocument('another_index', 'my_id', ['testField' => 'abc'], ['refresh' => 'true'])
        );
        $response = Promise\wait($this->client->catIndices(self::TEST_INDEX));
        $this->assertCount(1, $response);
        $this->assertEquals(self::TEST_INDEX, $response[0]['index']);
    }

    public function testCatHealth(): void
    {
        $response = Promise\wait($this->client->catHealth());
        $this->assertCount(1, $response);
        $this->assertArrayHasKey('status', $response[0]);
    }

    public function testRefreshOneIndex(): void
    {
        Promise\wait($this->client->createIndex(self::TEST_INDEX));
        $response = Promise\wait($this->client->refresh(self::TEST_INDEX));
        $this->assertCount(1, $response);
    }

    public function testRefreshManyIndices(): void
    {
        Promise\wait($this->client->createIndex('an_index'));
        Promise\wait($this->client->createIndex('another_index'));
        $response = Promise\wait($this->client->refresh('an_index,another_index'));
        $this->assertCount(1, $response);
    }

    public function testRefreshAllIndices(): void
    {
        Promise\wait($this->client->createIndex(self::TEST_INDEX));
        $response = Promise\wait($this->client->refresh());
        $this->assertCount(1, $response);
    }

    public function testSearch(): void
    {
        Promise\wait($this->client->createIndex(self::TEST_INDEX));
        Promise\wait(
            $this->client->indexDocument(self::TEST_INDEX, 'document-id', ['uuid' => 'this-is-a-uuid', 'payload' => []], ['refresh' => 'true'])
        );
        $query = [
            'term' => [
                'uuid.keyword' => [
                    'value' => 'this-is-a-uuid'
                ]
            ]
        ];
        $response = Promise\wait($this->client->search($query));
        $this->assertIsArray($response);
        $this->assertCount(1, $response['hits']['hits']);
    }

    public function testCount(): void
    {
        Promise\wait($this->client->createIndex(self::TEST_INDEX));
        Promise\wait(
            $this->client->indexDocument(self::TEST_INDEX, '', ['payload' => []], ['refresh' => 'true'])
        );
        Promise\wait(
            $this->client->indexDocument(self::TEST_INDEX, '', ['payload' => []], ['refresh' => 'true'])
        );

        $response = Promise\wait($this->client->count(self::TEST_INDEX));

        $this->assertIsArray($response);
        $this->assertEquals(2, $response['count']);
    }

    public function testCountWithQuery(): void
    {
        Promise\wait($this->client->createIndex(self::TEST_INDEX));
        Promise\wait(
            $this->client->indexDocument(self::TEST_INDEX, '', ['user' => 'kimchy'], ['refresh' => 'true'])
        );
        Promise\wait(
            $this->client->indexDocument(self::TEST_INDEX, '', ['user' => 'foo'], ['refresh' => 'true'])
        );

        $response = Promise\wait($this->client->count(self::TEST_INDEX, [], ['term' => ['user' => 'kimchy']]));

        $this->assertIsArray($response);
        $this->assertEquals(1, $response['count']);
    }

    public function testBulkIndex(): void
    {
        Promise\wait($this->client->createIndex(self::TEST_INDEX));
        $body = [];
        $responses = [];
        for ($i = 1; $i <= 1234; $i++) {
            $body[] = ['index' => ['_id' => '']];
            $body[] = ['test' => 'bulk', 'my_field' => 'my_value_' .  $i];

            // Every 100 documents stop and send the bulk request
            if ($i % 100 === 0) {
                $responses = Promise\wait($this->client->bulk($body, self::TEST_INDEX));
                $body = [];
                unset($responses);
            }
        }
        if (!empty($body)) {
            $responses = Promise\wait($this->client->bulk($body, self::TEST_INDEX));
        }

        $this->assertIsArray($responses);
        $this->assertCount(34, $responses['items']);
    }

    public function testCreateAlias(): void
    {
        Promise\wait($this->client->createIndex(self::TEST_INDEX));

        $response = Promise\wait(
            $this->client->createOrUpdateAlias(
                self::TEST_INDEX,
                'alias',
                ['filter' => ['term' => ['user' => 'kimchy']]]
            )
        );
        $this->assertIsArray($response);
        $this->assertTrue($response['acknowledged']);
        $response = Promise\wait($this->client->getIndex(self::TEST_INDEX));
        $this->assertEquals('kimchy', $response[self::TEST_INDEX]['aliases']['alias']['filter']['term']['user']);
    }

    public function testUpdateAlias(): void
    {
        Promise\wait($this->client->createIndex(self::TEST_INDEX));
        $this->client->createOrUpdateAlias(self::TEST_INDEX, 'alias');

        $response = Promise\wait(
            $this->client->createOrUpdateAlias(
                self::TEST_INDEX,
                'alias',
                ['filter' => ['term' => ['user' => 'kimchy']]]
            )
        );
        $this->assertIsArray($response);
        $this->assertTrue($response['acknowledged']);
        $response = Promise\wait($this->client->getIndex(self::TEST_INDEX));
        $this->assertEquals('kimchy', $response[self::TEST_INDEX]['aliases']['alias']['filter']['term']['user']);
    }

    public function testUpdateIndexSettings(): void
    {
        Promise\wait($this->client->createIndex(self::TEST_INDEX));

        $response = Promise\wait(
            $this->client->updateIndexSettings(
                self::TEST_INDEX,
                ['index' => ['mapping' => ['total_fields' => ['limit' => 2000]]]]
            )
        );

        $this->assertIsArray($response);
        $this->assertTrue($response['acknowledged']);
        $response = Promise\wait($this->client->getIndex(self::TEST_INDEX));
        $this->assertEquals(2000, $response[self::TEST_INDEX]['settings']['index']['mapping']['total_fields']['limit']);
    }

    public function testUpdateMappings(): void
    {
        Promise\wait($this->client->createIndex(self::TEST_INDEX));

        $response = Promise\wait(
            $this->client->updateMappings(self::TEST_INDEX, ['properties' => ['testField' => ['type' => 'text']]])
        );

        $this->assertIsArray($response);
        $this->assertTrue($response['acknowledged']);
        $response = Promise\wait($this->client->getIndex(self::TEST_INDEX));
        $this->assertEquals('text', $response[self::TEST_INDEX]['mappings']['properties']['testField']['type']);
    }
}
