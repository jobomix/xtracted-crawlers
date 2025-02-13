# syntax=docker/dockerfile:1
FROM --platform=$BUILDPLATFORM python:3.12.7-bullseye AS build
ARG TARGETOS
ARG TARGETARCH
WORKDIR /app

ENV POETRY_VIRTUALENVS_IN_PROJECT="true" \
	POETRY_NO_INTERACTION="true" \
	POETRY_HOME="/opt/poetry" \
	POETRY_VERSION="1.8.3"

ENV PATH="$PATH:$POETRY_HOME/bin"

COPY xtracted /app/xtracted
COPY poetry.lock /app
COPY pyproject.toml /app

RUN apt update && apt install bash python3-uvloop && curl -sSL https://install.python-poetry.org | python3 -

RUN mkdir -p -m 0600 ~/.ssh && ssh-keyscan github.com >> ~/.ssh/known_hosts

RUN cd /app
RUN --mount=type=ssh poetry install

RUN poetry run playwright install
RUN poetry run playwright install-deps

CMD [ "poetry", "run", "poe", "crawl_job_worker" ]

# FROM alpine
# COPY --from=build /app/server /server
# ENTRYPOINT ["/server"]
