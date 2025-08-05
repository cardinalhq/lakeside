
FROM eclipse-temurin:17-jdk-jammy AS builder
ARG APP_VERSION

COPY build/libs/lakeside-query-worker.jar application.jar

RUN java -Djarmode=layertools -jar application.jar extract

#having to do this special gymnasics because duckdb has one distro for arm and one for adm64  /w _gcc4
FROM eclipse-temurin:17-jdk-jammy AS build_arm64
ARG TARGETOS
ARG TARGETARCH

RUN mkdir -p /app/libs && curl -fsSL "https://extensions.duckdb.org/v1.3.2/${TARGETOS}_${TARGETARCH}/httpfs.duckdb_extension.gz" \
    | gunzip -c > /app/libs/httpfs.duckdb_extension

FROM eclipse-temurin:17-jdk-jammy AS build_amd64
ARG TARGETOS
ARG TARGETARCH

RUN mkdir -p /app/libs && curl -fsSL "https://extensions.duckdb.org/v1.3.2/${TARGETOS}_${TARGETARCH}/httpfs.duckdb_extension.gz" \
    | gunzip -c > /app/libs/httpfs.duckdb_extension

FROM build_${TARGETARCH}
ARG TARGETOS
ARG TARGETARCH

EXPOSE 7101

ADD https://github.com/open-telemetry/opentelemetry-java-instrumentation/releases/latest/download/opentelemetry-javaagent.jar /app/libs/otel-javaagent.jar

COPY libs/${TARGETOS}-${TARGETARCH}/lib-trigram.h /app/libs/lib-trigram.h
COPY libs/${TARGETOS}-${TARGETARCH}/lib-trigram.so /app/libs/lib-trigram.so

RUN chmod -R a+rx /app

COPY --from=builder dependencies/ ./
COPY --from=builder snapshot-dependencies/ ./
COPY --from=builder spring-boot-loader/ ./
COPY --from=builder application/ ./

COPY docker/run.sh /app/run.sh

CMD ["/app/run.sh"]
