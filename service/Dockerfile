FROM python:3.13-alpine3.23 AS builder
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

FROM python:3.13-alpine3.23 AS base

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

FROM python:3.13-alpine3.23 AS final

# VULN: patched 3.23/main builds the base image ships stale.
#   zlib: CVE-2026-4738 (fix 1.4-r0; the base still has 1.3.2-r1).
RUN apk upgrade --no-cache && apk add --no-cache --upgrade "zlib>=1.4-r0"

# VULN: sqlite-libs from edge — 3.23 ships 3.53.4-r0, flagged for CVE-2026-119373 /
# -119374 / -119379 / -119586. Edge is passed per command, not added to
# /etc/apk/repositories: a listed edge repo makes Trivy read the image as edge and
# flag musl/libuuid against edge's fix versions. No --upgrade: it would pull edge musl too.
RUN apk add --no-cache \
    --repository https://dl-cdn.alpinelinux.org/alpine/edge/main "sqlite-libs>=3.54.1"

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
