import boto3
import pytest
from aws_lambda_powertools.utilities import parameters
from moto import mock_dynamodb2

TABLE_NAME = "hogehoge"
DUMMY_ITEMS = 5  # ダミーデータを積む数


@pytest.fixture()
def mock_dynamodb_table(monkeypatch):
    mock_dynamodb2().start()

    dynamodb = boto3.resource("dynamodb")
    dynamodb.create_table(
        TableName=TABLE_NAME,
        KeySchema=[
            {"AttributeName": "hoge_pk", "KeyType": "HASH"},
        ],
        AttributeDefinitions=[
            {"AttributeName": "hoge_pk", "AttributeType": "N"},
        ],
        ProvisionedThroughput={"ReadCapacityUnits": 1, "WriteCapacityUnits": 1},
    )

    yield dynamodb.Table(TABLE_NAME)

    mock_dynamodb2().stop()


def put_dummy_data(table):
    """ダミーデータをテーブルに積む"""
    with table.batch_writer() as batch:
        for item in [dict(hoge_pk=i, hoge_value=f"hoge_{i}") for i in range(DUMMY_ITEMS)]:
            batch.put_item(Item=item)


def test_idempotency(mock_dynamodb_table):
    put_dummy_data(mock_dynamodb_table)

    dynamodb_provider = parameters.DynamoDBProvider(table_name=TABLE_NAME, key_attr="hoge_pk", value_attr="hoge_value")

    for i in range(DUMMY_ITEMS):
        value = dynamodb_provider.get(i)
        print(value)
        assert value == f"hoge_{i}"
