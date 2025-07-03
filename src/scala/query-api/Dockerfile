
FROM eclipse-temurin:17-jdk-jammy AS builder
ARG APP_VERSION

COPY build/libs/cardinal-backend-query-api-${APP_VERSION}.jar application.jar
RUN java -Djarmode=layertools -jar application.jar extract

FROM eclipse-temurin:17-jdk-jammy
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

CMD ["java", "-javaagent:/app/libs/otel-javaagent.jar", "-XX:+ExitOnOutOfMemoryError", "-XX:+UnlockDiagnosticVMOptions", "-XX:+DebugNonSafepoints", "-Dcom.sun.management.jmxremote", "-Dcom.sun.management.jmxremote.authenticate=false", "-Dcom.sun.management.jmxremote.ssl=false", "-Dcom.sun.management.jmxremote.port=9875", "-Dcom.sun.management.jmxremote.rmi.port=9875", "-Djava.rmi.server.hostname=localhost", "org.springframework.boot.loader.JarLauncher"]
