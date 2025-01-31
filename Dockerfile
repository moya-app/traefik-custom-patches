ARG DOCKER_HUB_URL
# WEBUI
FROM ${DOCKER_HUB_URL}library/node:22.9-alpine3.20 AS webui

ENV WEBUI_DIR=/src/webui
RUN mkdir -p $WEBUI_DIR

COPY ./webui/ $WEBUI_DIR/

WORKDIR $WEBUI_DIR

RUN yarn install --network-timeout 600000
RUN yarn build

# BUILD
FROM --platform=$BUILDPLATFORM ${DOCKER_HUB_URL}library/golang:1.23-alpine AS gobuild

# See https://docs.docker.com/build/building/multi-platform/#cross-compiling-a-go-application
ARG TARGETOS
ARG TARGETARCH

RUN apk --no-cache --no-progress add git mercurial bash gcc musl-dev curl tar ca-certificates tzdata \
    && update-ca-certificates \
    && rm -rf /var/cache/apk/*

WORKDIR /go/src/github.com/traefik/traefik

# Download go modules
COPY go.mod .
COPY go.sum .
RUN GO111MODULE=on GOPROXY=https://proxy.golang.org go mod download

COPY . /go/src/github.com/traefik/traefik

RUN rm -rf /go/src/github.com/traefik/traefik/webui/static/
COPY --from=webui /src/webui/static/ /go/src/github.com/traefik/traefik/webui/static/

ENV TRAEFIK_VERSION=v3.3.0
ENV CODENAME=cheddar

#RUN mkdir -p ./dist && ./script/make.sh binary

RUN mkdir -p dist && CGO_ENABLED=0 GOGC=off GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -ldflags "-s -w \
    -X github.com/traefik/traefik/v3/pkg/version.Version=${TRAEFIK_VERSION} \
    -X github.com/traefik/traefik/v3/pkg/version.Codename=${CODENAME} \
    -X github.com/traefik/traefik/v3/pkg/version.BuildDate=$(date -u '+%Y-%m-%d_%I:%M:%S%p')" \
    -installsuffix nocgo -o "./dist/linux/traefik" ./cmd/traefik


## IMAGE
FROM ${DOCKER_HUB_URL}library/alpine:3.21

RUN apk --no-cache --no-progress add bash curl ca-certificates tzdata \
    && update-ca-certificates \
    && rm -rf /var/cache/apk/*

COPY --from=gobuild /go/src/github.com/traefik/traefik/dist/linux/traefik /

EXPOSE 80
VOLUME ["/tmp"]

ENTRYPOINT ["/traefik"]
