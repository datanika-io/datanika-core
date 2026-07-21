# check=skip=SecretsUsedInArgOrEnv
#
# (The skip is for the ENV below: BuildKit flags any ENV named *API_KEY* as a
# baked secret. This one is a placeholder string, not a credential — see the
# comment at that line. Older BuildKit ignores the directive as a comment.)
#
# Image Glama build-tests to produce a release for the directory listing (core#402).
#
# Glama only needs the server to start and answer an MCP introspection request:
# all 25 tools register statically via @mcp.tool(), the httpx client opens no
# connection at construction time, and main() only checks that an API key is
# *present*. So no live Datanika instance is required to build or introspect.
#
# Build context is the REPO ROOT (the COPY below reaches into datanika-mcp/):
#   docker build -f datanika-mcp/Dockerfile -t datanika-mcp .
FROM python:3.12-slim

WORKDIR /srv
COPY datanika-mcp/ /srv/
RUN pip install --no-cache-dir .

# Not a credential: a placeholder that lets the stdio server start for
# introspection. A real deployment passes --api-key / DATANIKA_API_KEY.
ENV DATANIKA_URL=https://app.datanika.io \
    DATANIKA_API_KEY=glama-introspection-placeholder

ENTRYPOINT ["datanika-mcp"]
