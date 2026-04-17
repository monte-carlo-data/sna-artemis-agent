import base64
import binascii
import logging

logger = logging.getLogger(__name__)

TENANT_US1 = "us1"
TENANT_EU1 = "eu1"
DEFAULT_TENANT = TENANT_US1


def parse_tenant_from_id(mcd_id: str) -> str:
    """Decode the tenant from the ``mcd_id`` credential.

    The ``mcd_id`` contains a ``+`` and what's after it is a base64 encoded
    string with the format ``v1+<tenant>`` (for example ``v1+us1`` or
    ``v1+eu1``). Returns the tenant string. Falls back to
    :data:`DEFAULT_TENANT` when the id is missing, has no ``+``, or decoding
    fails.
    """
    if not mcd_id or "+" not in mcd_id:
        return DEFAULT_TENANT

    try:
        encoded_tenant = mcd_id.split("+")[1]
        decoded = base64.b64decode(encoded_tenant).decode("utf-8")
        if "+" not in decoded:
            return decoded
        return decoded.split("+")[1]
    except (IndexError, binascii.Error, UnicodeDecodeError):
        logger.exception("Failed to decode base64 tenant value")
        return DEFAULT_TENANT
