#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

###############################
# Builder stage: package distribution with Maven Daemon
###############################
FROM maven:3.9.7-eclipse-temurin-17 AS builder

ENV MAVEN_OPTS="-Xmx8g -Xms1g"

WORKDIR /workspace
COPY . .

# Package the distribution to produce apache-seatunnel-*-bin.tar.gz
RUN mvn -B clean package \
    -pl seatunnel-dist -am \
    -D"skip.ui"=true \
    -Dmaven.test.skip=true \
    -D"skip.spotless"=true \
    -Prelease \
    --no-transfer-progress

###############################
# Runtime image: include built distribution
###############################
FROM azul/zulu-openjdk:17

ENV DOCKER=true \
    TZ=Asia/Shanghai \
    SEATUNNEL_HOME=/opt/seatunnel

RUN apt-get update && \
    apt-get install -y --no-install-recommends curl ca-certificates && \
    rm -rf /var/lib/apt/lists/*

# Copy built distribution from builder
COPY --from=builder /workspace/seatunnel-dist/target/apache-seatunnel-*-bin.tar.gz /opt/

WORKDIR /opt
RUN mkdir seatunnel && \
    tar -xzf apache-seatunnel-*-bin.tar.gz -C seatunnel --strip-components=1 && \
    rm -f /opt/apache-seatunnel-*-bin.tar.gz

WORKDIR /opt/seatunnel

EXPOSE 5801

CMD [ "/bin/bash", "./bin/seatunnel-cluster.sh" ]
