import base64
from unittest import TestCase

from agent.utils.backend_url import resolve_backend_url


def _encode(value: str) -> str:
    return base64.b64encode(value.encode("utf-8")).decode("utf-8")


def _id_for(tenant: str) -> str:
    return f"some-id+{_encode(f'v1+{tenant}')}"


_US1_ID = _id_for("us1")
_EU1_ID = _id_for("eu1")

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

    def test_arbitrary_region_tenant(self):
        # Future regional instance — the agent doesn't need a code change to
        # construct the URL because the Snowflake EAI uses a wildcard host.
        self.assertEqual(
            "https://artemis.ap1.getmontecarlo.com:443",
            resolve_backend_url(_id_for("ap1"), _PROD_FALLBACK),
        )

    def test_short_letters_only_tenant(self):
        # Tenant identifiers can be short letter-only strings so they should
        # also resolve to a host-spliced URL.
        self.assertEqual(
            "https://artemis.abc.getmontecarlo.com:443",
            resolve_backend_url(_id_for("abc"), _PROD_FALLBACK),
        )
        self.assertEqual(
            "https://artemis.xy.getmontecarlo.com:443",
            resolve_backend_url(_id_for("xy"), _PROD_FALLBACK),
        )

    def test_tenant_with_hyphen(self):
        self.assertEqual(
            "https://artemis.some-tenant.getmontecarlo.com:443",
            resolve_backend_url(_id_for("some-tenant"), _PROD_FALLBACK),
        )

    def test_invalid_tenant_format_raises(self):
        # A tenant with URL-significant characters must not be spliced into
        # the host. parse_tenant_from_id returns the decoded payload as-is
        # when there's no "v1+" prefix, so we use that path to feed a junk
        # tenant string through the resolver.
        with self.assertRaises(ValueError) as ctx:
            resolve_backend_url(f"some-id+{_encode('bad/tenant')}", _PROD_FALLBACK)
        self.assertIn("bad/tenant", str(ctx.exception))

    def test_non_artemis_fallback_raises(self):
        with self.assertRaises(ValueError):
            resolve_backend_url(_EU1_ID, "https://not-artemis.getmontecarlo.com:443")
