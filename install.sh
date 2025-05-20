#!/bin/bash

pushd "$(dirname "$0")"

mkdir -p ./binary
pushd ./binary

VERSION="25.4.4.25"

case $(uname -m) in
  x86_64) ARCH=amd64 ;;         # For Intel/AMD 64-bit processors
  aarch64) ARCH=arm64 ;;        # For ARM 64-bit processors
  *) echo "Unknown architecture $(uname -m)"; exit 1 ;; # Exit if architecture isn't supported
esac

PKGS="clickhouse-common-static clickhouse-server"

for PKG in $PKGS
do
    URL="https://packages.clickhouse.com/tgz/stable/$PKG-$VERSION-$ARCH.tgz" 
    GENERIC_URL="https://packages.clickhouse.com/tgz/stable/$PKG-$VERSION.tgz"

    curl -L -fO $URL || curl -L -fO $GENERIC_URL
done

for PKG in $PKGS
do
    FILE="$PKG-$VERSION-$ARCH.tgz"
    GENERIC_FILE="$PKG-$VERSION.tgz"

    tar -xzf $FILE || tar -xzf $GENERIC_FILE
done

for PKG in $PKGS
do
    "$PKG-$VERSION/install/doinst.sh" configure
done

popd
popd

