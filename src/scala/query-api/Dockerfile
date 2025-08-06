# Copyright (C) 2025 CardinalHQ, Inc
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, version 3.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

FROM eclipse-temurin:17-jdk-jammy AS builder
ARG APP_VERSION

COPY build/libs/lakeside-query-api.jar application.jar
RUN java -Djarmode=layertools -jar application.jar extract

FROM eclipse-temurin:17-jdk-jammy
ARG TARGETOS
ARG TARGETARCH

EXPOSE 7101

ADD https://github.com/open-telemetry/opentelemetry-java-instrumentation/releases/latest/download/opentelemetry-javaagent.jar /app/libs/otel-javaagent.jar

COPY libs/${TARGETOS}-${TARGETARCH}/lib-trigram.h /app/libs/lib-trigram.h
COPY libs/${TARGETOS}-${TARGETARCH}/lib-trigram.so /app/libs/lib-trigram.so

COPY docker/run.sh /app/run.sh

RUN chmod -R a+rx /app

COPY --from=builder dependencies/ ./
COPY --from=builder snapshot-dependencies/ ./
COPY --from=builder spring-boot-loader/ ./
COPY --from=builder application/ ./

CMD ["/app/run.sh"]
