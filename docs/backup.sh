#!/usr/bin/env bash

WORKSPACE=$(cd `dirname $0`/; pwd)
BACKUP_DIRECTORY=${WORKSPACE}/versions/

create_backup_directory(){
  if [[ ! -d "${BACKUP_DIRECTORY}" ]];then
    mkdir ${BACKUP_DIRECTORY}
  fi
}

backup() {
  create_backup_directory
  cp ${WORKSPACE}/$1 ${BACKUP_DIRECTORY}/$1_`date '+%Y%m%d_%H%M%S'`
}

backup $1