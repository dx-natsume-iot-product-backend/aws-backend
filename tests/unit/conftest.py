import boto3
import pytest


@pytest.fixture(scope="session")
def localstack_option():
    return {
        "endpoint_url": "http://localhost:4566",
        "region_name": "ap-northeast-1",
        "aws_access_key_id": "dummy",
        "aws_secret_access_key": "dummy",
    }


@pytest.fixture(scope="session")
def dynamodb_resource(localstack_option):
    return boto3.resource("dynamodb", **localstack_option)


@pytest.fixture(scope="function")
def create_table(request, dynamodb_resource):
    name: str = request.param
    option = {
        "TableName": name,
        "AttributeDefinitions": [
            {"AttributeName": "deviceId", "AttributeType": "S"},
            {"AttributeName": "startTimestamp", "AttributeType": "S"},
        ],
        "KeySchema": [
            {"AttributeName": "deviceId", "KeyType": "HASH"},
            {"AttributeName": "startTimestamp", "KeyType": "RANGE"},
        ],
        "ProvisionedThroughput": {"ReadCapacityUnits": 1, "WriteCapacityUnits": 1},
    }
    table = dynamodb_resource.create_table(**option)

    yield

    table.delete()


@pytest.fixture(scope="function")
def put_items(request, dynamodb_resource):
    name = request.param["table_name"]
    items = request.param["items"]

    table = dynamodb_resource.Table(name)
    with table.batch_writer() as batch:
        for item in items:
            batch.put_item(item)


@pytest.fixture(scope="function")
def set_environ(request, monkeypatch):
    for k, v in request.param.items():
        monkeypatch.setenv(k, v)
