FROM python:3.13-alpine3.21 AS builder
ENV VIRTUAL_ENV=/opt/venv
RUN python -m venv $VIRTUAL_ENV
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

# libraries required to build cryptography and other requirements
RUN apk add build-base gcc musl-dev python3-dev libffi-dev openssl-dev cargo pkgconfig git

COPY requirements-build.txt .
RUN pip install -r requirements-build.txt

# VULN: patch venv build tooling.
#   setuptools: CVE-2025-47273.
#   wheel:      CVE-2026-24049; >=0.48.0 for GHSA-vgq5-9859-3mmw (`wheel convert` path traversal, affecting 0.9.6-0.47.0).
#   pip:        CVE-2026-8643 / CVE-2026-6357 / CVE-2026-3219 / CVE-2026-1703 (fix 26.1.2).
RUN pip install --no-cache-dir --upgrade "pip>=26.1.2" "setuptools>=81.0.0" "wheel>=0.48.0"

# GHSA-vgq5-9859-3mmw: setuptools vendors its own wheel (0.46.3), inside the
# affected range and unreachable by the upgrade above — setuptools pins what it
# vendors. Nothing here builds wheels, so drop it; the code goes too, not just
# the dist-info, or convert.py stays on disk with only its metadata hidden.
RUN rm -rf $VIRTUAL_ENV/lib/python*/site-packages/setuptools/_vendor/wheel \
           $VIRTUAL_ENV/lib/python*/site-packages/setuptools/_vendor/wheel-*.dist-info

FROM python:3.13-alpine3.21 AS base

COPY --from=builder /opt/venv /opt/venv
ENV VIRTUAL_ENV=/opt/venv
ENV PATH="/opt/venv/bin:$PATH"

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY gunicorn.conf.py ./
COPY agent/ ./agent

ARG code_version="local"
ARG build_number="0"
RUN echo $code_version,$build_number > ./agent/utils/version

FROM base AS tests

COPY requirements-dev.txt .
RUN pip install --no-cache-dir -r requirements-dev.txt

COPY tests/ ./tests

ARG pytest_params="tests"
ARG CACHEBUST=1
RUN python -m pytest $pytest_params

FROM python:3.13-alpine3.21 AS final

# update openssl
RUN apk update && apk add --no-cache openssl~3.3

# VULN: CVE-2026-40200 - upgrade musl (and musl-utils, same source pkg) to patched 3.21 release
RUN apk add --no-cache --upgrade "musl>=1.2.5-r11" "musl-utils>=1.2.5-r11"

# VULN: CVE-2026-22184 / CVE-2026-27171 - upgrade zlib to the patched 3.21 release.
# The base image still carries 1.3.1-r2; 3.21/main has 1.3.2-r0, so this stays
# above the edge repo added below.
RUN apk add --no-cache --upgrade "zlib>=1.3.2-r0"

# VULN: CVE-2025-46394 / CVE-2024-58251 (busybox, fixed in r14) and CVE-2026-34743
# (xz, fixed in 5.8.3-r0). busybox-binsh and ssl_client ship from the same source
# package and apk won't move them unless named, like the musl pair above. Also
# takes CVE-2025-60876 out of scope: alpine records no fix for it, but its
# affected range ends at r13. Must stay above the edge repo too — with edge in
# the list, a bare --upgrade pulls busybox 1.38.x and musl 1.2.6 instead.
RUN apk add --no-cache --upgrade "busybox>=1.37.0-r14" "busybox-binsh>=1.37.0-r14" \
                                 "ssl_client>=1.37.0-r14" "xz-libs>=5.8.3-r0"

# VULN-557 - Upgrade sqlite-libs to 3.49.1
RUN echo "http://dl-cdn.alpinelinux.org/alpine/edge/main" >> /etc/apk/repositories
RUN apk add "sqlite-libs>=3.49.1"

RUN adduser --disabled-password mcdagent

COPY --from=base --chown=mcdagent /opt/venv /opt/venv
COPY --from=base --chown=mcdagent /agent /agent
COPY --from=base --chown=mcdagent /gunicorn.conf.py /gunicorn.conf.py

# VULN: no pip at runtime — pip's vendored msgpack/setuptools are flagged
# (CVE-2026-57585 / GHSA-6v7p-g79w-8964 / CVE-2025-47273) and the latest pip
# still bundles them. Nothing installs packages at runtime; builder/base/tests
# keep pip. Uninstalling via pip itself avoids version-specific venv paths.
RUN /opt/venv/bin/python -m pip uninstall -y pip \
 && /usr/local/bin/python -m pip uninstall -y pip

USER mcdagent

ENV VIRTUAL_ENV=/opt/venv
ENV PATH="/opt/venv/bin:$PATH"

CMD ["gunicorn", "--config", "gunicorn.conf.py", "agent.main:app", "--bind", "0.0.0.0:8000"]
