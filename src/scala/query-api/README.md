## Compiling Loki
```
- kubectl exec --stdin --tty ingestion-service-app-0 -- /bin/bash
- microdnf install git
- microdnf install go
- ssh-keygen -t rsa -b 4096 -C "<github user email for e.g. ruchir.jha@gmail.com>"
- Add generated key to github SSH keys
- git clone git@github.com:icefloat/loki.git
- cd loki/pkg/logql/main
- go build -buildmode=c-shared -o bin/lib-loki.so run_parser.go
- kubectl cp ingestion-service-app-0:loki/pkg/logql/main/bin/lib-loki.h lib-loki.h
- kubectl cp ingestion-service-app-0:loki/pkg/logql/main/bin/lib-loki.so lib-loki.so
```
