#!/bin/bash

function out.indent() {
	sed 's/^/       /g'
}

CWD=${PWD}

FILENAME=$1

TORUN=$CWD/$FILENAME

echo "File path: ${TORUN}" | out.indent

cd ~/PATH_TO_SPARK/

./bin/spark-submit $TORUN