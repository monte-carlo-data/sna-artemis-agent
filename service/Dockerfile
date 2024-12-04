FROM python:3.12-alpine3.20 AS builder
ENV VIRTUAL_ENV=/opt/venv
RUN python -m venv $VIRTUAL_ENV
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

# libraries required to build cryptography and other requirements
RUN apk add build-base gcc musl-dev python3-dev libffi-dev openssl-dev cargo pkgconfig

COPY requirements-build.txt .
RUN pip install -r requirements-build.txt

FROM python:3.12-alpine3.20 AS base

COPY --from=builder /opt/venv /opt/venv
ENV VIRTUAL_ENV=/opt/venv
ENV PATH="/opt/venv/bin:$PATH"

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

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

FROM python:3.12-alpine3.20 AS final

# update openssl
RUN apk update && apk add --no-cache openssl~3.3

# upgrade global libraries to fix CVEs
RUN pip install --no-cache-dir --upgrade pip==24.2 && \
    pip install --no-cache-dir --upgrade setuptools==75.1.0

RUN adduser --disabled-password mcdagent

COPY --from=base --chown=mcdagent /opt/venv /opt/venv
COPY --from=base --chown=mcdagent /agent /agent

USER mcdagent

ENV VIRTUAL_ENV=/opt/venv
ENV PATH="/opt/venv/bin:$PATH"

CMD ["gunicorn", "agent.main:app", "--bind", "0.0.0.0:8000"]
