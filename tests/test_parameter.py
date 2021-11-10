import boto3
import pytest
from aws_lambda_powertools.utilities import parameters
from moto import mock_dynamodb2

TABLE_NAME = "hogehoge"
DUMMY_ITEMS = 5  # ダミーデータを積む数

# ======================== テスト用のテーブルとダミーデータ↓


@pytest.fixture()
def initialize(monkeypatch):

    mock_dynamodb2().start()

    # テスト用のテーブルを作成する
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
    table = dynamodb.Table(TABLE_NAME)

    # ダミーデータをテーブルに積む
    for item in [dict(hoge_pk=i, hoge_value=f"hoge_{i}") for i in range(DUMMY_ITEMS)]:
        table.put_item(Item=item)

    yield

    mock_dynamodb2().stop()


# ======================== ↓動作確認用（LambdaPowertoolsを使った場合）


def test_dynamodb_parameters(initialize):

    dynamodb_provider = parameters.DynamoDBProvider(table_name=TABLE_NAME, key_attr="hoge_pk", value_attr="hoge_value")

    for i in range(DUMMY_ITEMS):
        value = dynamodb_provider.get(i)

        print(value)
        assert value == f"hoge_{i}"


# ======================== ↓動作確認用（Boto3を使った場合）


def test_boto3(initialize):

    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table(TABLE_NAME)

    for i in range(DUMMY_ITEMS):
        value = table.get_item(Key={"hoge_pk": i})["Item"]["hoge_value"]

        print(value)
        assert value == f"hoge_{i}"
