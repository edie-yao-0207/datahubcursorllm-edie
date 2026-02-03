# https://github.com/datahub-project/datahub/blob/master/docker/mysql-setup/Dockerfile
# this Dockerfile will be updated dynamically by the build script using the selected release tag
# this file will later be removed and the code will be refactored to use a list of images to build

# Defining custom repo urls for use in enterprise environments. Re-used between stages below.
ARG ALPINE_REPO_URL=http://dl-cdn.alpinelinux.org/alpine

FROM 228413409843.dkr.ecr.us-west-2.amazonaws.com/golang-alpine:alpine3.20__latest AS binary

ARG ALPINE_REPO_URL

ENV DOCKERIZE_VERSION v0.6.1
WORKDIR /go/src/github.com/jwilder

# Optionally set corporate mirror for apk
RUN if [ "${ALPINE_REPO_URL}" != "http://dl-cdn.alpinelinux.org/alpine" ] ; then sed -i "s#http.*://dl-cdn.alpinelinux.org/alpine#${ALPINE_REPO_URL}#g" /etc/apk/repositories ; fi

RUN apk --no-cache --update add openssl git tar curl

WORKDIR /go/src/github.com/jwilder/dockerize

RUN go install github.com/jwilder/dockerize@$DOCKERIZE_VERSION

FROM 228413409843.dkr.ecr.us-west-2.amazonaws.com/alpine:3.20__latest
COPY --from=binary /go/bin/dockerize /usr/local/bin

ARG ALPINE_REPO_URL

# Optionally set corporate mirror for apk
RUN if [ "${ALPINE_REPO_URL}" != "http://dl-cdn.alpinelinux.org/alpine" ] ; then sed -i "s#http.*://dl-cdn.alpinelinux.org/alpine#${ALPINE_REPO_URL}#g" /etc/apk/repositories ; fi

# PFP-260: Upgrade Sqlite to >=3.28.0-r0 to fix https://security.snyk.io/vuln/SNYK-ALPINE39-SQLITE-449762
RUN apk add --no-cache mysql-client bash mariadb-connector-c sqlite

COPY docker/mysql-setup/init.sql /init.sql
COPY docker/mysql-setup/init.sh /init.sh
RUN chmod 755 init.sh

ENV DATAHUB_DB_NAME="datahub"

CMD dockerize -wait tcp://$MYSQL_HOST:$MYSQL_PORT -timeout 240s /init.sh
