import base64
from unittest import TestCase

from agent.utils.backend_url import resolve_backend_url


def _encode(value: str) -> str:
    return base64.b64encode(value.encode("utf-8")).decode("utf-8")


_US1_ID = f"some-id+{_encode('v1+us1')}"
_EU1_ID = f"some-id+{_encode('v1+eu1')}"
_OTHER_ID = f"some-id+{_encode('v1+ap1')}"

_PROD_FALLBACK = "https://artemis.getmontecarlo.com:443"
_DEV_FALLBACK = "https://artemis.dev.getmontecarlo.com:443"


class ResolveBackendUrlTests(TestCase):
    def test_us1_returns_fallback(self):
        self.assertEqual(_PROD_FALLBACK, resolve_backend_url(_US1_ID, _PROD_FALLBACK))

    def test_us1_returns_fallback_dev(self):
        self.assertEqual(_DEV_FALLBACK, resolve_backend_url(_US1_ID, _DEV_FALLBACK))

    def test_no_plus_treated_as_us1(self):
        self.assertEqual(_PROD_FALLBACK, resolve_backend_url("no-plus", _PROD_FALLBACK))

    def test_empty_mcd_id_treated_as_us1(self):
        self.assertEqual(_PROD_FALLBACK, resolve_backend_url("", _PROD_FALLBACK))

    def test_eu1_prod(self):
        self.assertEqual(
            "https://artemis.eu1.getmontecarlo.com:443",
            resolve_backend_url(_EU1_ID, _PROD_FALLBACK),
        )

    def test_eu1_dev(self):
        self.assertEqual(
            "https://artemis.eu1.dev.getmontecarlo.com:443",
            resolve_backend_url(_EU1_ID, _DEV_FALLBACK),
        )

    def test_eu1_without_port_is_preserved(self):
        self.assertEqual(
            "https://artemis.eu1.getmontecarlo.com",
            resolve_backend_url(_EU1_ID, "https://artemis.getmontecarlo.com"),
        )

    def test_unsupported_tenant_raises(self):
        with self.assertRaises(ValueError) as ctx:
            resolve_backend_url(_OTHER_ID, _PROD_FALLBACK)
        self.assertIn("ap1", str(ctx.exception))

    def test_eu1_with_non_artemis_fallback_raises(self):
        with self.assertRaises(ValueError):
            resolve_backend_url(_EU1_ID, "https://not-artemis.getmontecarlo.com:443")
