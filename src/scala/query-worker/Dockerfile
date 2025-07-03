
FROM eclipse-temurin:17-jdk-jammy AS builder
ARG APP_VERSION

COPY build/libs/cardinal-backend-query-worker-${APP_VERSION}.jar application.jar

RUN java -Djarmode=layertools -jar application.jar extract

#having to do this special gymnasics because duckdb has one distro for arm and one for adm64  /w _gcc4
FROM eclipse-temurin:17-jdk-jammy AS build_arm64
ARG TARGETOS
ARG TARGETARCH

RUN mkdir -p /app/libs && curl -fsSL "https://extensions.duckdb.org/v1.2.0/${TARGETOS}_${TARGETARCH}/httpfs.duckdb_extension.gz" \
    | gunzip -c > /app/libs/httpfs.duckdb_extension

FROM eclipse-temurin:17-jdk-jammy AS build_amd64
ARG TARGETOS
ARG TARGETARCH

RUN mkdir -p /app/libs && curl -fsSL "https://extensions.duckdb.org/v1.2.0/${TARGETOS}_${TARGETARCH}_gcc4/httpfs.duckdb_extension.gz" \
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

CMD ["java", "-javaagent:/app/libs/otel-javaagent.jar", "-XX:+ExitOnOutOfMemoryError", "-XX:+UnlockDiagnosticVMOptions", "-XX:+DebugNonSafepoints", "-Dcom.sun.management.jmxremote", "-Dcom.sun.management.jmxremote.authenticate=false", "-Dcom.sun.management.jmxremote.ssl=false", "-Dcom.sun.management.jmxremote.port=9875", "-Dcom.sun.management.jmxremote.rmi.port=9875", "-Djava.rmi.server.hostname=localhost", "-Dnetworkaddress.cache.ttl=120", "org.springframework.boot.loader.JarLauncher"]
