#!/usr/bin/env bash

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

JQ=/usr/bin/jq
EXTRA_INSTANCE_DATA=/emr/instance-controller/lib/info/extraInstanceData.json

# TODO BONFIRE-2707: Set these properties in instance-controller so that all
# processes can benefit from them, not just Spark.
if [ -x $jq ] && [ -e $EXTRA_INSTANCE_DATA ]; then
  export EMR_CLUSTER_ID=$($JQ -r '.jobFlowId' $EXTRA_INSTANCE_DATA)
  export EMR_RELEASE_LABEL=$($JQ -r '.releaseLabel' $EXTRA_INSTANCE_DATA)
fi

EXTRA_OPTS=
[ -n "$EMR_CLUSTER_ID" ] && EXTRA_OPTS+=" -DEMR_CLUSTER_ID=$EMR_CLUSTER_ID"
[ -n "$EMR_RELEASE_LABEL" ] && EXTRA_OPTS+=" -DEMR_RELEASE_LABEL=$EMR_RELEASE_LABEL"
# EMR_STEP_ID is added to the environment by instance-controller when a step is run
[ -n "$EMR_STEP_ID" ] && EXTRA_OPTS+=" -DEMR_STEP_ID=$EMR_STEP_ID"

SPARK_SUBMIT_OPTS+=$EXTRA_OPTS
SPARK_DAEMON_JAVA_OPTS+=$EXTRA_OPTS

export SPARK_SUBMIT_OPTS
export SPARK_DAEMON_JAVA_OPTS
