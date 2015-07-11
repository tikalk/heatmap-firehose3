#!/bin/bash

echo "Starting $0"

DIRNAME=`dirname $0`
PROCESS_HOME=`cd $DIRNAME/.;pwd;`
export PROCESS_HOME;


java  -Dvertx.logger-delegate-factory-class-name=io.vertx.core.logging.Log4jLogDelegateFactory -Dlog4j.configuration=file:$PROCESS_HOME/conf/log4j.properties -jar $PROCESS_HOME/build/libs/heatmap-firehose3-3.0.0-fat.jar -conf $PROCESS_HOME/conf/conf.json