
FROM eclipse-temurin:17-jdk-jammy AS builder

COPY build/libs/lakeside-query-worker.jar application.jar
RUN java -Djarmode=layertools -jar application.jar extract

FROM eclipse-temurin:17-jdk-jammy

ARG TARGETOS
ARG TARGETARCH

EXPOSE 7101

COPY libs/${TARGETOS}-${TARGETARCH}/lib-trigram.h /app/libs/lib-trigram.h
COPY libs/${TARGETOS}-${TARGETARCH}/lib-trigram.so /app/libs/lib-trigram.so

RUN curl -fsSL "https://extensions.duckdb.org/v1.3.2/${TARGETOS}_${TARGETARCH}/httpfs.duckdb_extension.gz" \
    | gunzip -c > /app/libs/httpfs.duckdb_extension

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
