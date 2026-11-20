# Built by GoReleaser (dockers_v2 in .goreleaser.yaml), which puts the
# binary it already compiled for each platform at <os>/<arch>/asyncapi-gen
# in the build context. To build it yourself, run:
#   goreleaser release --snapshot --clean
FROM scratch
ARG TARGETPLATFORM
COPY $TARGETPLATFORM/asyncapi-gen /usr/local/bin/asyncapi-gen
WORKDIR /work
ENTRYPOINT ["/usr/local/bin/asyncapi-gen"]
