
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
