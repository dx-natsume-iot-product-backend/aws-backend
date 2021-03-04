import pytest

import put_dynamodb as index


class TestParseEvent(object):
    @pytest.mark.parametrize(
        "event, expected",
        [
            (
                {"deviceId": "test-device", "timestamp": "1234567890"},
                ("test-device", "1234567890"),
            )
        ],
    )
    def test_normal(self, event, expected):
        actual = index.parse_event(event)
        assert actual == expected


class TestGetTableName(object):
    @pytest.mark.parametrize(
        "set_environ, expected",
        [({"TABLE_NAME": "test_table"}, "test_table")],
        indirect=["set_environ"],
    )
    @pytest.mark.usefixtures("set_environ")
    def test_normal(self, expected):
        actual = index.get_table_name()
        assert actual == expected


class TestGetPreviousItem(object):
    @pytest.mark.parametrize(
        "create_table, put_items, table_name, device_id, expected",
        [
            (
                "test_table",
                {"table_name": "test_table", "items": []},
                "test_table",
                "test-device",
                None,
            ),
            (
                "test_table",
                {
                    "table_name": "test_table",
                    "items": [
                        {"deviceId": "test-device", "startTimestamp": "1234567890"}
                    ],
                },
                "test_table",
                "test-device",
                {"deviceId": "test-device", "startTimestamp": "1234567890"},
            ),
            (
                "test_table",
                {
                    "table_name": "test_table",
                    "items": [
                        {"deviceId": "test-device", "startTimestamp": "1234567890"},
                        {"deviceId": "test-device", "startTimestamp": "1234567899"},
                    ],
                },
                "test_table",
                "test-device",
                {"deviceId": "test-device", "startTimestamp": "1234567899"},
            ),
            (
                "test_table",
                {
                    "table_name": "test_table",
                    "items": [
                        {"deviceId": "test-device", "startTimestamp": "1234567890"},
                        {
                            "deviceId": "test-device",
                            "startTimestamp": "1234567899",
                            "endTimestamp": "1234567900",
                        },
                        {
                            "deviceId": "test-device-2",
                            "startTimestamp": "1234567900",
                            "endTimestamp": "1234567900",
                        },
                    ],
                },
                "test_table",
                "test-device",
                {
                    "deviceId": "test-device",
                    "startTimestamp": "1234567899",
                    "endTimestamp": "1234567900",
                },
            ),
            (
                "test_table",
                {
                    "table_name": "test_table",
                    "items": [
                        {
                            "deviceId": "test-device",
                            "startTimestamp": "2021-03-03 11:00:00.000000+00:00",
                        },
                        {
                            "deviceId": "test-device",
                            "startTimestamp": "2021-03-03 12:00:00.000000+00:00",
                        },
                        {
                            "deviceId": "test-device-2",
                            "startTimestamp": "2021-03-03 13:00:00.000000+00:00",
                        },
                    ],
                },
                "test_table",
                "test-device",
                {
                    "deviceId": "test-device",
                    "startTimestamp": "2021-03-03 12:00:00.000000+00:00",
                },
            ),
        ],
        indirect=["create_table", "put_items"],
    )
    @pytest.mark.usefixtures("create_table", "put_items")
    def test_normal(self, dynamodb_resource, table_name, device_id, expected):
        actual = index.get_previous_item(table_name, device_id, dynamodb_resource)
        assert actual == expected


class TestIsEnd(object):
    @pytest.mark.parametrize(
        "item, expected",
        [(None, True), ({}, False), ({"endTimestamp": "1234567890"}, True)],
    )
    def test_normal(self, item, expected):
        actual = index.is_end(item)
        assert actual == expected


class TestPutStartItem(object):
    @pytest.mark.parametrize(
        "create_table, table_name, device_id, timestamp, expected",
        [
            (
                "test_table",
                "test_table",
                "test-device",
                "1234567890",
                {"deviceId": "test-device", "startTimestamp": "1234567890"},
            )
        ],
        indirect=["create_table"],
    )
    @pytest.mark.usefixtures("create_table")
    def test_normal(
        self, dynamodb_resource, table_name, device_id, timestamp, expected
    ):
        index.put_start_item(table_name, device_id, timestamp, dynamodb_resource)

        resp = dynamodb_resource.Table(table_name).get_item(
            Key={"deviceId": device_id, "startTimestamp": timestamp}
        )
        assert resp["Item"] == expected


class TestUpdateEndItem(object):
    @pytest.mark.parametrize(
        "create_table, put_items, table_name, device_id, start_timestamp, end_timestamp, expected",
        [
            (
                "test_table",
                {
                    "table_name": "test_table",
                    "items": [
                        {"deviceId": "test-device", "startTimestamp": "1234567890"}
                    ],
                },
                "test_table",
                "test-device",
                "1234567890",
                "2234567890",
                {
                    "deviceId": "test-device",
                    "startTimestamp": "1234567890",
                    "endTimestamp": "2234567890",
                },
            )
        ],
        indirect=["create_table", "put_items"],
    )
    @pytest.mark.usefixtures("create_table", "put_items")
    def test_normal(
        self,
        dynamodb_resource,
        table_name,
        device_id,
        start_timestamp,
        end_timestamp,
        expected,
    ):
        actual = index.update_end_item(
            table_name, device_id, start_timestamp, end_timestamp, dynamodb_resource
        )
        assert actual == expected


class TestMain(object):
    @pytest.mark.parametrize(
        "set_environ, create_table, put_items, event, expected_table_name, expected_device_id, expected_start_timestamp, expected_item",
        [
            (
                {"TABLE_NAME": "test_table"},
                "test_table",
                {"table_name": "test_table", "items": []},
                {"deviceId": "test-device", "timestamp": "1234567890"},
                "test_table",
                "test-device",
                "1234567890",
                {"deviceId": "test-device", "startTimestamp": "1234567890"},
            ),
            (
                {"TABLE_NAME": "test_table"},
                "test_table",
                {
                    "table_name": "test_table",
                    "items": [
                        {"deviceId": "test-device", "startTimestamp": "1234567880"}
                    ],
                },
                {"deviceId": "test-device", "timestamp": "1234567890"},
                "test_table",
                "test-device",
                "1234567880",
                {
                    "deviceId": "test-device",
                    "startTimestamp": "1234567880",
                    "endTimestamp": "1234567890",
                },
            ),
            (
                {"TABLE_NAME": "test_table"},
                "test_table",
                {
                    "table_name": "test_table",
                    "items": [
                        {
                            "deviceId": "test-device",
                            "startTimestamp": "1234567880",
                            "endTimestamp": "1234567888",
                        }
                    ],
                },
                {"deviceId": "test-device", "timestamp": "1234567890"},
                "test_table",
                "test-device",
                "1234567890",
                {
                    "deviceId": "test-device",
                    "startTimestamp": "1234567890",
                },
            ),
            (
                {"TABLE_NAME": "test_table"},
                "test_table",
                {
                    "table_name": "test_table",
                    "items": [
                        {
                            "deviceId": "test-device",
                            "startTimestamp": "1134567890",
                            "endTimestamp": "1144567890",
                        },
                        {
                            "deviceId": "test-device",
                            "startTimestamp": "1234567880",
                            "endTimestamp": "1234567888",
                        },
                    ],
                },
                {"deviceId": "test-device", "timestamp": "1234567890"},
                "test_table",
                "test-device",
                "1234567890",
                {
                    "deviceId": "test-device",
                    "startTimestamp": "1234567890",
                },
            ),
            (
                {"TABLE_NAME": "test_table"},
                "test_table",
                {
                    "table_name": "test_table",
                    "items": [
                        {
                            "deviceId": "test-device",
                            "startTimestamp": "1134567890",
                            "endTimestamp": "1144567890",
                        },
                        {"deviceId": "test-device", "startTimestamp": "1234567880"},
                    ],
                },
                {"deviceId": "test-device", "timestamp": "1234567890"},
                "test_table",
                "test-device",
                "1234567880",
                {
                    "deviceId": "test-device",
                    "startTimestamp": "1234567880",
                    "endTimestamp": "1234567890",
                },
            ),
        ],
        indirect=["set_environ", "create_table", "put_items"],
    )
    @pytest.mark.usefixtures("set_environ", "create_table", "put_items")
    def test_normal(
        self,
        dynamodb_resource,
        event,
        expected_table_name,
        expected_device_id,
        expected_start_timestamp,
        expected_item,
    ):
        index.main(event, dynamodb_resource=dynamodb_resource)

        resp = dynamodb_resource.Table(expected_table_name).get_item(
            Key={
                "deviceId": expected_device_id,
                "startTimestamp": expected_start_timestamp,
            }
        )
        assert resp["Item"] == expected_item
