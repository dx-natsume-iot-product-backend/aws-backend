import os
from logging import DEBUG, Logger, getLogger
from typing import Optional, Tuple

import boto3
from boto3.dynamodb.conditions import Key
from boto3.resources.base import ServiceResource

KEY_EVENT_DEVICE_ID = "deviceId"
KEY_EVENT_TIMESTAMP = "timestamp"

KEY_TABLE_DEVICE_ID = "deviceId"
KEY_TABLE_START_TIMESTAMP = "startTimestamp"
KEY_TABLE_END_TIMESTAMP = "endTimestamp"


logger = getLogger(__name__)
logger.setLevel(DEBUG)


def handler(event, context):
    try:
        main(event)
    except Exception as e:
        logger.error(f"error occured: {e}", exc_info=True)
        raise


def main(event, dynamodb_resource: ServiceResource = boto3.resource("dynamodb")):
    logger.debug("event", extra={"additional_data": {"event": event}})
    device_id, timestamp = parse_event(event)
    table_name = get_table_name()
    previous_item = get_previous_item(table_name, device_id, dynamodb_resource)

    if is_end(previous_item):
        put_start_item(table_name, device_id, timestamp, dynamodb_resource)
    else:
        update_end_item(
            table_name,
            device_id,
            previous_item[KEY_TABLE_START_TIMESTAMP],
            timestamp,
            dynamodb_resource,
        )


def parse_event(event) -> Tuple[str, str]:
    device_id = event[KEY_EVENT_DEVICE_ID]
    timestamp = event[KEY_EVENT_TIMESTAMP]
    return device_id, timestamp


def get_table_name():
    return os.environ["TABLE_NAME"]


def get_previous_item(
    table_name: str, device_id: str, dynamodb_resource: ServiceResource
) -> Optional[dict]:
    table = dynamodb_resource.Table(table_name)
    option = {
        "KeyConditionExpression": Key(KEY_TABLE_DEVICE_ID).eq(device_id),
        "ScanIndexForward": False,
        "Limit": 1,
        "ProjectionExpression": "#deviceId, #startTimestamp, #endTimestamp",
        "ExpressionAttributeNames": {
            "#deviceId": KEY_TABLE_DEVICE_ID,
            "#startTimestamp": KEY_TABLE_START_TIMESTAMP,
            "#endTimestamp": KEY_TABLE_END_TIMESTAMP,
        },
    }
    resp = table.query(**option)
    logger.debug(
        "[get_previous_item] query result", extra={"additional_data": {"result": resp}}
    )

    items = resp.get("Items", [])
    return items[0] if len(items) == 1 else None


def is_end(item: Optional[dict]) -> bool:
    if item is None:
        return True
    return KEY_TABLE_END_TIMESTAMP in item


def put_start_item(
    table_name: str, device_id: str, timestamp: str, dynamodb_resource: ServiceResource
):
    table = dynamodb_resource.Table(table_name)
    option = {
        "Item": {KEY_TABLE_DEVICE_ID: device_id, KEY_TABLE_START_TIMESTAMP: timestamp},
    }
    table.put_item(**option)


def update_end_item(
    table_name: str,
    device_id: str,
    start_timestamp: str,
    end_timestamp: str,
    dynamodb_resource: ServiceResource,
) -> dict:
    table = dynamodb_resource.Table(table_name)
    option = {
        "Key": {
            KEY_TABLE_DEVICE_ID: device_id,
            KEY_TABLE_START_TIMESTAMP: start_timestamp,
        },
        "UpdateExpression": "set #endTimestamp = :endTimestamp",
        "ExpressionAttributeNames": {"#endTimestamp": KEY_TABLE_END_TIMESTAMP},
        "ExpressionAttributeValues": {":endTimestamp": end_timestamp},
        "ReturnValues": "ALL_NEW",
    }
    resp = table.update_item(**option)
    return resp.get("Attributes", {})
