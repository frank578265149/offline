#!/usr/bin/env bash


#Find the java binary

SCALA_VERSION=2.10
#Figure out where XMHT is installed
FWDIR="$(cd `dirname $0`/..; pwd)"
export PIO_HOME="${FWDIR}"

. ${FWDIR}/bin/load-pio-env.sh

echo "the app dir is : ${PIO_HOME}"
if [ -n "${JAVA_HOME}" ]; then
      RUNNER="${JAVA_HOME}/bin/java"
else
      if [ `command -v java` ]; then
              RUNNER="java"
      else
              echo -e "\033[0;31mJAVA_HOME is not set\033[0m" >&2
              exit 1
      fi
fi
# Compute classpath using external script
classpath_output=$(${FWDIR}/bin/compute-classpath.sh)
if [[ "$?" != "0" ]]; then
  echo "$classpath_output"
  exit 1
else
  CLASSPATH=${classpath_output}
fi
echo "classpath :${classpath_output}"
export CLASSPATH

exec "$RUNNER" -cp "$CLASSPATH"  "$@"