#!/bin/sh
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
