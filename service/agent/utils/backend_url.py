import logging
from urllib.parse import urlparse, urlunparse

from agent.utils.tenant import TENANT_EU1, TENANT_US1, parse_tenant_from_id

logger = logging.getLogger(__name__)

_ARTEMIS_HOST_PREFIX = "artemis."
_EU1_HOST_PREFIX = "artemis.eu1."


def resolve_backend_url(mcd_id: str, fallback_url: str) -> str:
    """Resolve the backend URL for the tenant encoded in ``mcd_id``.

    The ``fallback_url`` is the URL baked into the image at deploy time and
    doubles as the US1 URL and the env signal (e.g. ``artemis.getmontecarlo.com``
    for prod, ``artemis.dev.getmontecarlo.com`` for dev). For ``us1`` we return
    the fallback. For ``eu1`` we derive the EU host by splicing ``eu1.`` after
    ``artemis.``. Any other tenant raises :class:`ValueError` — there is no
    silent fallback.
    """
    tenant = parse_tenant_from_id(mcd_id)
    if tenant == TENANT_US1:
        return fallback_url
    if tenant == TENANT_EU1:
        return _rewrite_host_for_eu1(fallback_url)
    raise ValueError(
        f"Unsupported tenant {tenant!r} resolved from mcd_id; only "
        f"{TENANT_US1!r} and {TENANT_EU1!r} are supported."
    )


def _rewrite_host_for_eu1(fallback_url: str) -> str:
    parsed = urlparse(fallback_url)
    host = parsed.hostname or ""
    if not host.startswith(_ARTEMIS_HOST_PREFIX):
        raise ValueError(
            f"Cannot derive EU backend URL: fallback host {host!r} does not "
            f"start with {_ARTEMIS_HOST_PREFIX!r}."
        )
    new_host = _EU1_HOST_PREFIX + host[len(_ARTEMIS_HOST_PREFIX) :]
    netloc = f"{new_host}:{parsed.port}" if parsed.port else new_host
    return urlunparse(parsed._replace(netloc=netloc))
