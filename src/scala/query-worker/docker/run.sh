#!/bin/sh

extrajars=""

baseargs="\
  -XX:+ExitOnOutOfMemoryError \
  -XX:+UnlockDiagnosticVMOptions \
  -XX:+DebugNonSafepoints \
  -Dcom.sun.management.jmxremote \
  -Dcom.sun.management.jmxremote.authenticate=false \
  -Dcom.sun.management.jmxremote.ssl=false \
  -Dcom.sun.management.jmxremote.port=9875 \
  -Dcom.sun.management.jmxremote.rmi.port=9875 \
  -Djava.rmi.server.hostname=localhost \
  org.springframework.boot.loader.JarLauncher"

if [ "$ENABLE_OTLP_TELEMETRY" = "true" ]; then
  extrajars="${extrajars} -javaagent:/app/libs/otel-javaagent.jar"
  echo "OTLP Telemetry exporting enabled"
fi

exec java ${baseargs} ${extrajars} "$@"
