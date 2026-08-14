FROM python:3.13-alpine3.21 AS builder
ENV VIRTUAL_ENV=/opt/venv
RUN python -m venv $VIRTUAL_ENV
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

# libraries required to build cryptography and other requirements
RUN apk add build-base gcc musl-dev python3-dev libffi-dev openssl-dev cargo pkgconfig git

COPY requirements-build.txt .
RUN pip install -r requirements-build.txt

# VULN: patch venv build tooling.
#   setuptools: CVE-2025-47273 — also >=81.0.0 vendors patched wheel 0.46.3 under setuptools/_vendor/.
#   wheel:      CVE-2026-24049.
#   pip:        CVE-2026-8643 / CVE-2026-6357 / CVE-2026-3219 / CVE-2026-1703 (fix 26.1.2).
RUN pip install --no-cache-dir --upgrade "pip>=26.1.2" "setuptools>=81.0.0" "wheel>=0.47.0"

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

# VULN-557 - Upgrade sqlite-libs to 3.49.1
RUN echo "http://dl-cdn.alpinelinux.org/alpine/edge/main" >> /etc/apk/repositories
RUN apk add "sqlite-libs>=3.49.1"

RUN adduser --disabled-password mcdagent

COPY --from=base --chown=mcdagent /opt/venv /opt/venv
COPY --from=base --chown=mcdagent /agent /agent
COPY --from=base --chown=mcdagent /gunicorn.conf.py /gunicorn.conf.py

# VULN: drop pip from the runtime image entirely. Nothing installs packages at
# runtime (gunicorn serves the app out of the venv), and pip's bundled
# third-party tree is what scanners flag:
#   pip/_vendor/msgpack 1.1.2 — CVE-2026-57585 / GHSA-6v7p-g79w-8964, no pip
#     release vendors the patched 1.2.1 yet (26.2.1 is the latest).
#   pip/_vendor/setuptools 70.3.0 — CVE-2025-47273, same situation.
# Removing pip also retires the earlier global-pip upgrade that only existed to
# close pip's own CVEs (CVE-2026-8643 / CVE-2026-6357 / CVE-2026-3219 /
# CVE-2026-1703). The builder/base/tests stages keep pip and are unchanged.
#
# pip uninstalls itself, so there are no interpreter paths to keep in sync with
# the Python version — and if a future base image ships pip differently, this
# fails the build instead of silently leaving pip in place.
RUN /opt/venv/bin/python -m pip uninstall -y pip \
 && /usr/local/bin/python -m pip uninstall -y pip

USER mcdagent

ENV VIRTUAL_ENV=/opt/venv
ENV PATH="/opt/venv/bin:$PATH"

CMD ["gunicorn", "--config", "gunicorn.conf.py", "agent.main:app", "--bind", "0.0.0.0:8000"]
