#!/bin/bash
go mod tidy
go mod vendor
export PYTHONUNBUFFERED=0
export DH_GOLANG_INSTALL_EXTRA='vendor'
dpkg-buildpackage -b
cp -a ../*.deb ./_build/