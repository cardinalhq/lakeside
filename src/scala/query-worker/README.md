Query-worker ECS cluster information:

Region: us-east-2

#####

cardinalhq-internal (prod)

list tasks:
```
aws ecs list-tasks --cluster cardinalhq-queries
```

https://us-east-2.console.aws.amazon.com/ecs/v2/clusters/cardinalhq-queries/services?region=us-east-2

ECS cluster: cardinalhq-queries
arn:aws:ecs:us-east-2:033263751764:cluster/cardinalhq-queries

Service: query-worker
arn:aws:ecs:us-east-2:033263751764:service/cardinalhq-queries/query-worker

Task Definition: query-worker-prod:11
arn:aws:ecs:us-east-2:033263751764:task-definition/query-worker-prod:11

#####

cardinalhq-dev

list tasks:
```
aws ecs list-tasks --cluster cardinalhq-dev-queries
```

https://us-east-2.console.aws.amazon.com/ecs/v2/clusters/cardinalhq-dev-queries/services?region=us-east-2

ECS cluster: cardinalhq-dev-queries
arn:aws:ecs:us-east-2:033263751764:cluster/cardinalhq-dev-queries

Service: query-worker
arn:aws:ecs:us-east-2:033263751764:service/cardinalhq-dev-queries/query-worker

Task Definition: query-worker:18
arn:aws:ecs:us-east-2:033263751764:task-definition/query-worker:18


