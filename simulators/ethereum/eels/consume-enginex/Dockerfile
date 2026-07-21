# Builds and runs the EELS (execution-specs) consume enginex simulator
FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim AS base

## Default fixtures/git-ref
ARG fixtures=stable@latest
ENV FIXTURES=${fixtures}
ARG branch=""

## Clone and install EELS
RUN apt-get update && apt-get install -y git

# Clone repo and use the default branch if none specified
RUN git clone --depth 1 https://github.com/ethereum/execution-specs.git && \
    cd execution-specs && \
    if [ -n "$branch" ]; then \
    git fetch --depth 1 origin "$branch" && \
    git checkout FETCH_HEAD; \
    fi

WORKDIR /execution-specs/packages/testing
RUN uv sync

# Optionally bake pre-staged fixtures into the image: stage ./fixtures.tar.gz
# and pass fixtures=/fixtures to have `consume` read them locally; otherwise
# FIXTURES stays a release/URL and is downloaded as before. The throwaway
# stage keeps the tarball out of the final image; the fixtures=/fixtures gate
# keeps a staged-but-unused tarball from bloating it; the `Dockerfile` anchor
# lets COPY succeed when nothing is staged; --strip-components=1 drops the
# tarball's top-level dir (EEST release layout) so the shipped index is reused.
FROM base AS fixtures-stage
COPY Dockerfile fixtures.tar.gz* /staged/
RUN mkdir -p /fixtures && \
    if [ "$FIXTURES" = "/fixtures" ] && [ -f /staged/fixtures.tar.gz ]; then \
        tar -xzf /staged/fixtures.tar.gz -C /fixtures --strip-components=1; \
    fi

FROM base
COPY --from=fixtures-stage /fixtures /fixtures

# Cache the fixtures at build time so container starts don't re-download them
# (for pre-staged fixtures this only validates them and reuses the shipped
# index). Rebuild the image (`--docker.nocache`) to pick up newer fixtures.
RUN uv run consume cache --input "$FIXTURES"


## Define `consume enginex` entry point using the local fixtures
# TODO: remove when fixed on nimbus-el side.
ARG disable_strict_exception_matching=nimbus-el
ENV DISABLE_STRICT_EXCEPTION_MATCHING=${disable_strict_exception_matching}
ENTRYPOINT uv run consume enginex -v --input "$FIXTURES" --disable-strict-exception-matching "$DISABLE_STRICT_EXCEPTION_MATCHING"
