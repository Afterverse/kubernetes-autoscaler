#!/usr/bin/env bash

BREW_PREFIX=$(brew --prefix)

# fix path for gnu tools
export PATH=$BREW_PREFIX/opt/coreutils/libexec/gnubin:$PATH
export PATH=$BREW_PREFIX/opt/grep/libexec/gnubin:$PATH
export PATH=$BREW_PREFIX/opt/gnu-getopt/bin:$PATH
export PATH=$BREW_PREFIX/opt/gnu-sed/libexec/gnubin:$PATH
export PATH=$BREW_PREFIX/opt/findutils/libexec/gnubin:$PATH

# fix go location
export PATH=$BREW_PREFIX/opt/go@1.12/bin:$PATH

KUBE_ROOT=$(dirname "${BASH_SOURCE}")/..

TEMP_DIR=/tmp/ca-update-vendor.7SA1
REVISION=c00066ea6295a43be2f18207fd532dffce5df0ce

mkdir -p $TEMP_DIR

bash $KUBE_ROOT/hack/update-vendor.sh -d$TEMP_DIR -r$REVISION
