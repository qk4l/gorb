FROM golang:1.23

ENV DEBIAN_FRONTEND=noninteractive
RUN  apt-get update \
  && apt-get install -y software-properties-common python3-pip \
  build-essential \
  libssl-dev \
  libffi-dev
RUN apt-get install --no-install-suggests --no-install-recommends -y \
  git \
  python3-netaddr \
  unzip \
  vim \
  dh-golang \
  golang-any

# Grab the source code and add it to the workspace.
ENV PATHWORK=/go/src/github.com/qk4l/gorb
WORKDIR $PATHWORK

ADD ./docker/entrypoint.sh /entrypoint.sh
RUN chmod 755 /entrypoint.sh
CMD /entrypoint.sh
