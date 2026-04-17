import base64
from unittest import TestCase

from agent.utils.tenant import (
    DEFAULT_TENANT,
    TENANT_EU1,
    TENANT_US1,
    parse_tenant_from_id,
)


def _encode(value: str) -> str:
    return base64.b64encode(value.encode("utf-8")).decode("utf-8")


class ParseTenantFromIdTests(TestCase):
    def test_empty_returns_default(self):
        self.assertEqual(DEFAULT_TENANT, parse_tenant_from_id(""))

    def test_missing_plus_returns_default(self):
        self.assertEqual(DEFAULT_TENANT, parse_tenant_from_id("no-plus-here"))

    def test_valid_us1(self):
        mcd_id = f"some-id+{_encode('v1+us1')}"
        self.assertEqual(TENANT_US1, parse_tenant_from_id(mcd_id))

    def test_valid_eu1(self):
        mcd_id = f"some-id+{_encode('v1+eu1')}"
        self.assertEqual(TENANT_EU1, parse_tenant_from_id(mcd_id))

    def test_invalid_base64_returns_default(self):
        mcd_id = "some-id+not-valid-base64!!!"
        self.assertEqual(DEFAULT_TENANT, parse_tenant_from_id(mcd_id))

    def test_decoded_without_version_prefix_returns_decoded(self):
        # If the decoded payload is just the tenant (no "v1+" prefix) we
        # return the decoded value as-is — matches the fallback branch in
        # parse_tenant_from_id.
        mcd_id = f"some-id+{_encode('eu1')}"
        self.assertEqual("eu1", parse_tenant_from_id(mcd_id))

    def test_non_utf8_decoded_returns_default(self):
        mcd_id = f"some-id+{base64.b64encode(bytes([0xFF, 0xFE, 0xFD])).decode()}"
        self.assertEqual(DEFAULT_TENANT, parse_tenant_from_id(mcd_id))
