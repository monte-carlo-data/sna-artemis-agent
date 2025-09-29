import json
from datetime import datetime, date
from decimal import Decimal
from unittest import TestCase
from agent.utils.serde import AgentSerializer, ATTRIBUTE_NAME_TYPE, ATTRIBUTE_NAME_DATA


class SerdeTests(TestCase):
    def test_datetime_serialization(self):
        value = datetime.now()
        result = AgentSerializer.serialize(value)
        self.assertTrue(isinstance(result, dict))
        self.assertEqual(result.get(ATTRIBUTE_NAME_TYPE), "datetime")
        self.assertTrue(isinstance(result.get(ATTRIBUTE_NAME_DATA), str))

    def test_time_serialization(self):
        value = datetime.now().time()
        result = AgentSerializer.serialize(value)
        self.assertTrue(isinstance(result, dict))
        self.assertEqual(result.get(ATTRIBUTE_NAME_TYPE), "time")
        self.assertTrue(isinstance(result.get(ATTRIBUTE_NAME_DATA), str))

    def test_time_json_serialization(self):
        value = {
            "time": datetime.now().time(),
        }
        str_value = json.dumps(
            {
                "result": value,
            },
            cls=AgentSerializer,
        )
        json_value = json.loads(str_value)
        self.assertTrue(isinstance(json_value, dict))
        self.assertTrue(isinstance(json_value.get("result"), dict))
        self.assertTrue(isinstance(json_value.get("result").get("time"), dict))
        self.assertEqual(
            json_value.get("result").get("time").get(ATTRIBUTE_NAME_TYPE), "time"
        )
