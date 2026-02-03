# https://github.com/datahub-project/datahub/blob/master/docker/elasticsearch-setup/Dockerfile
# this Dockerfile will be updated dynamically by the build script using the selected release tag
# this file will later be removed and the code will be refactored to use a list of images to build

# This "container" is a workaround to pre-create search indices

# Defining environment
ARG APP_ENV=prod

# Defining custom repo urls for use in enterprise environments. Re-used between stages below.
ARG ALPINE_REPO_URL=http://dl-cdn.alpinelinux.org/alpine

FROM 228413409843.dkr.ecr.us-west-2.amazonaws.com/golang-alpine:alpine3.20__latest AS binary

ARG ALPINE_REPO_URL

ENV DOCKERIZE_VERSION v0.6.1
WORKDIR /go/src/github.com/jwilder

# Optionally set corporate mirror for apk
RUN if [ "${ALPINE_REPO_URL}" != "http://dl-cdn.alpinelinux.org/alpine" ] ; then sed -i "s#http.*://dl-cdn.alpinelinux.org/alpine#${ALPINE_REPO_URL}#g" /etc/apk/repositories ; fi

# PFP-260: Upgrade Sqlite to >=3.28.0-r0 to fix https://security.snyk.io/vuln/SNYK-ALPINE39-SQLITE-449762
RUN apk --no-cache --update add openssl git tar curl sqlite

WORKDIR /go/src/github.com/jwilder/dockerize

RUN go install github.com/jwilder/dockerize@$DOCKERIZE_VERSION

FROM 228413409843.dkr.ecr.us-west-2.amazonaws.com/alpine:3.20__latest AS base

ARG ALPINE_REPO_URL

# Optionally set corporate mirror for apk
RUN if [ "${ALPINE_REPO_URL}" != "http://dl-cdn.alpinelinux.org/alpine" ] ; then sed -i "s#http.*://dl-cdn.alpinelinux.org/alpine#${ALPINE_REPO_URL}#g" /etc/apk/repositories ; fi

RUN apk add --no-cache curl jq bash coreutils
COPY --from=binary /go/bin/dockerize /usr/local/bin

FROM base AS prod-install

COPY docker/elasticsearch-setup/create-indices.sh /
RUN chmod 755 create-indices.sh
COPY metadata-service/restli-servlet-impl/src/main/resources/index /index

FROM base AS dev-install
# Dummy stage for development. Use local files for setup
# See this excellent thread https://github.com/docker/cli/issues/1134

FROM ${APP_ENV}-install AS final

CMD if [ "$ELASTICSEARCH_USE_SSL" == "true" ]; then ELASTICSEARCH_PROTOCOL=https; else ELASTICSEARCH_PROTOCOL=http; fi \
    && if [[ -n "$ELASTICSEARCH_USERNAME" ]]; then ELASTICSEARCH_HTTP_HEADERS="Authorization: Basic $(echo -ne "$ELASTICSEARCH_USERNAME:$ELASTICSEARCH_PASSWORD" | base64)"; else ELASTICSEARCH_HTTP_HEADERS="Accept: */*"; fi \
    && if [[ "$SKIP_ELASTICSEARCH_CHECK" != "true" ]]; then \
        dockerize -wait $ELASTICSEARCH_PROTOCOL://$ELASTICSEARCH_HOST:$ELASTICSEARCH_PORT -wait-http-header "${ELASTICSEARCH_HTTP_HEADERS}" -timeout 120s /create-indices.sh; \
    else /create-indices.sh; fi
