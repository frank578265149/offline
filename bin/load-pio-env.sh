#!/usr/bin/env bash

#This script loads pio.env.sh if it exists and ensures it is only loaded  once.
#pio-env.sh is loaded from XMHT_CONF_DIR if set or within the current directory's
# conf/ subdirectory
#Figure out where XMHT-BIGDATA is installed

FWDIR="$(cd `dirname $0`/..; pwd)"
#Export this  an  XMHT_HOME
export XMHT_HOME="${FWDIR}"

if [ -z "$XMHT_ENV_LOADED" ]; then
   export XMHT_ENV_LOADED=1
   #Returns the parent of the directory this script lives in
   parent_dir="$(cd `dirname $0`/..; pwd)"
   use_conf_dir=${PIO_CONF_DIR:-"${parent_dir}/conf"}
   echo $use_conf_dir
    if [ -f "${use_conf_dir}/pio-env.sh" ]; then
      # Promote all variable declarations to environment (exported) variables
      echo "promote all variable declarations to environment exported variables"
      set -a
      . "${use_conf_dir}/pio-env.sh"
      set +a
    else
       echo -e "\033[0;35mWarning: pio-env.sh was not found in ${use_conf_dir}. Using system environment variables instead.\033[0m\n"
    fi
fi