#!/bin/bash
go mod tidy
go build
export PYTHONUNBUFFERED=0
dpkg-buildpackage -b
cp -a ../*.deb ./_build/
#while /bin/true; do /autocompile.py $PWD ".go" "make binary" ; done
