FROM openjdk:8-jre-alpine

ENV SBT_VERSION 1.0.3

# install sbt
RUN apk add curl tar && \
      mkdir -p /opt/sbt && \
      curl -SL https://piccolo.link/sbt-${SBT_VERSION}.tgz | tar -xzC /opt/sbt && \
      apk del curl tar

# enable local-preloaded repo
RUN mkdir ~/.sbt/ && ln -sf /opt/sbt/sbt/lib/local-preloaded/ ~/.sbt/preloaded

# dependencies of sbt
RUN apk add bash bc

# dependencies of open_images_downloader
RUN apk add imagemagick

COPY . /open_images_downloader/
WORKDIR /open_images_downloader

# build
RUN /opt/sbt/sbt/bin/sbt stage

ENTRYPOINT ["/open_images_downloader/target/universal/stage/bin/open_images_downloader"]
