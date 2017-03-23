#!/usr/bin/env bash

search() {
  local i=0;
  local needle=$1;
  shift
  for str in $@; do
    if [ "$str" = "$needle" ]; then
      echo ${i}
      return
    else
      ((i++))
    fi
  done
  echo ${i}
}
export PIO_HOME="$(cd `dirname $0`/..; pwd)"

export PIO_CONF_DIR="$PIO_HOME/conf"

PIDFILE=$1
shift
#echo "$@"
FIRST_SEP=$(search "--" $@)
#echo "$FIRST_SEP"
FIRST_HALF="${@:1:$FIRST_SEP}"
#echo $FIRST_HALF
SECOND_HALF="${@:$FIRST_SEP+1}"
#echo $SECOND_HALF
exec   ${PIO_HOME}/bin/bigdata-class.sh bigdata.analysis.scala.console.Console  ${FIRST_HALF} --pio-home ${PIO_HOME} ${SECOND_HALF}
#\<&- > /dev/null 2>&1 &
echo $! > ${PIDFILE}