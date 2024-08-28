FROM golang:1.23

ENV DEBIAN_FRONTEND=noninteractive
RUN  apt-get update \
  && apt-get install -y software-properties-common python3-pip \
  build-essential \
  libssl-dev \
  libffi-dev
RUN apt-get install --no-install-suggests --no-install-recommends -y \
  curl \
  git \
  build-essential \
  python3-netaddr \
  unzip \
  vim \
  wget \
  inotify-tools \
  dh-golang \
  golang-any 

# Grab the source code and add it to the workspace.
ENV PATHWORK=/go/src/github.com/qk4l/gorb
ADD ./ $PATHWORK
WORKDIR $PATHWORK

ADD ./docker/* /
RUN chmod 755 /entrypoint.sh
RUN chmod 755 /autocompile.py
CMD /entrypoint.sh
