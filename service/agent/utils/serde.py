import base64
import dataclasses
import json
from datetime import datetime, date
from decimal import Decimal
from typing import Any, Dict, Union

ATTRIBUTE_NAME_TYPE = "__type__"
ATTRIBUTE_NAME_DATA = "__data__"
ATTRIBUTE_VALUE_TYPE_BYTES = "bytes"
ATTRIBUTE_VALUE_TYPE_DATETIME = "datetime"
ATTRIBUTE_VALUE_TYPE_DATE = "date"
ATTRIBUTE_VALUE_TYPE_DECIMAL = "decimal"
ATTRIBUTE_NAME_TRACE_ID = "__mcd_trace_id__"
ATTRIBUTE_NAME_RESULT = "__mcd_result__"
ATTRIBUTE_NAME_ERROR = "__mcd_error__"
ATTRIBUTE_NAME_ERROR_TYPE = "__mcd_error_type__"
ATTRIBUTE_NAME_ERROR_ATTRS = "__mcd_error_attrs__"


class AgentSerializer(json.JSONEncoder):
    @classmethod
    def serialize(cls, value: Any) -> Any:
        if isinstance(value, datetime):
            return {
                ATTRIBUTE_NAME_TYPE: ATTRIBUTE_VALUE_TYPE_DATETIME,
                ATTRIBUTE_NAME_DATA: value.isoformat(),
            }
        elif isinstance(value, date):
            return {
                ATTRIBUTE_NAME_TYPE: ATTRIBUTE_VALUE_TYPE_DATE,
                ATTRIBUTE_NAME_DATA: value.isoformat(),
            }
        elif isinstance(value, Decimal):
            return {
                ATTRIBUTE_NAME_TYPE: ATTRIBUTE_VALUE_TYPE_DECIMAL,
                ATTRIBUTE_NAME_DATA: str(value),
            }
        elif isinstance(value, bytes) or isinstance(value, bytearray):
            return {
                ATTRIBUTE_NAME_TYPE: ATTRIBUTE_VALUE_TYPE_BYTES,
                ATTRIBUTE_NAME_DATA: base64.b64encode(value).decode("utf-8"),
            }
        elif dataclasses.is_dataclass(value):
            return dataclasses.asdict(value)

        return value

    def default(self, obj: Any):
        serialized = self.serialize(obj)
        if serialized is not obj:  # serialization happened
            return serialized
        return super().default(obj)


def decode_dict_value(value: Dict) -> Any:
    if value.get(ATTRIBUTE_NAME_TYPE) == ATTRIBUTE_VALUE_TYPE_BYTES:
        return base64.b64decode(value.get(ATTRIBUTE_NAME_DATA))  # type: ignore
    return value


def decode_dictionary(dict_value: Dict) -> Dict:
    def decode_deep(value: Any) -> Any:
        if isinstance(value, Dict):
            return (
                decode_dict_value(value)
                if ATTRIBUTE_NAME_TYPE in value
                else decode_dictionary(value)
            )
        else:
            return value

    return {key: decode_deep(value) for key, value in dict_value.items()}
