import logging
import re
from urllib.parse import urlparse, urlunparse

from agent.utils.tenant import TENANT_US1, parse_tenant_from_id

logger = logging.getLogger(__name__)

_ARTEMIS_HOST_PREFIX = "artemis."
# Tenants are short identifiers like "us1", "eu1", "ap1" or short customer
# identifiers. The pattern allows alphanumerics + hyphens (DNS-safe,
# case-insensitive) so we can construct backend URLs for new regions and
# customer tenants without an agent code change. The check still rejects
# empty / URL-unsafe strings so a malformed mcd_id can't produce something
# like `artemis..getmontecarlo.com`.
_TENANT_PATTERN = re.compile(r"^[A-Za-z0-9-]+$")


def resolve_backend_url(mcd_id: str, fallback_url: str) -> str:
    """Resolve the backend URL for the tenant encoded in ``mcd_id``.

    The ``fallback_url`` is the URL baked into the image at deploy time and
    doubles as the US1 URL and the env signal (e.g. ``artemis.getmontecarlo.com``
    for prod, ``artemis.dev.getmontecarlo.com`` for dev). For ``us1`` we return
    the fallback. For any other tenant we splice ``<tenant>.`` after
    ``artemis.`` in the fallback URL — Snowflake's external access integration
    uses the wildcard ``artemis.*.getmontecarlo.com`` so the resulting URL is
    already authorized at the network layer.

    Raises :class:`ValueError` if the tenant string is empty or contains
    characters outside ``[A-Za-z0-9-]``, or if the fallback URL doesn't start
    with ``artemis.``.
    """
    tenant = parse_tenant_from_id(mcd_id)
    if tenant == TENANT_US1:
        return fallback_url
    return _rewrite_host_for_tenant(fallback_url, tenant)


def _rewrite_host_for_tenant(fallback_url: str, tenant: str) -> str:
    if not _TENANT_PATTERN.fullmatch(tenant):
        raise ValueError(
            f"Refusing to build backend URL for tenant {tenant!r}; expected a "
            f"non-empty identifier matching {_TENANT_PATTERN.pattern!r} "
            f"(e.g. 'us1', 'eu1', 'ap1')."
        )
    parsed = urlparse(fallback_url)
    host = parsed.hostname or ""
    if not host.startswith(_ARTEMIS_HOST_PREFIX):
        raise ValueError(
            f"Cannot derive backend URL for tenant {tenant!r}: fallback host "
            f"{host!r} does not start with {_ARTEMIS_HOST_PREFIX!r}."
        )
    new_host = f"{_ARTEMIS_HOST_PREFIX}{tenant}." + host[len(_ARTEMIS_HOST_PREFIX) :]
    netloc = f"{new_host}:{parsed.port}" if parsed.port else new_host
    return urlunparse(parsed._replace(netloc=netloc))
