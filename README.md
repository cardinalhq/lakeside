
## Architecture

<img width="688" alt="Screen Shot 2023-01-13 at 9 19 54 AM" src="https://user-images.githubusercontent.com/6104857/212233361-fa3558d1-45df-4d74-9400-83037dd7de9f.png">


## Ingestion Service


1. Ingestion Service is fronted by a Load Balancer (Address: `http://ingestion-service-app.default.svc.cluster.local:7101`). Deploy Script = `ingestion/deploy.sh`
2. Ingestion Service uses consistent hashing to divide the data between `N` ingest nodes
3. Since any node can receive the incoming batch by virtue of being behind the Load Balancer, we have the ability to
   route the data to the appropriate node (achieved by placing these nodes into a Consistent Hashing Ring)
4. The nodes communicate with each other over persistent WebSocket connections forming a mesh network. 
5. This ring is maintained by periodically polling Kubernetes Service Discovery.
6. We then buffer data into a (local) Write Ahead Log (WAL) which becomes queryable 5s after it gets written to.
7. The WAL is stored on an attached kubernetes persistent volume. This file is referred to as an `unsealed segment`
8. Every 20 minutes the WAL gets flushed & closed, converted to parquet and uploaded to S3. This file is referred to as
   a `sealed segment`. 

## Query Service
1. Query Service is fronted by a Load Balancer (Address: `http://query-api.default.svc.cluster.local:7101`). Deploy Script = `query-api/deploy.sh`
2. Every incoming expression is broken down into sub or data expressions. 
3. For each data expression, we consult the Query Index (index-api) and identify the list of interesting segments. 
4. The segment list per `dataExpr` is broken down into `sealed` and `unsealed` lists. 
5. The `unsealed` list indicates the actively written to segments. The Query Service sends the unsealed list to each ingestion
   node. Each ingestion node identifies what segments it possesses locally and then starts applying the DataExpr to them.
   As each row is read from the unsealed segment it's streamed back to the Query Service over the WebSocket connection.
6. Similarly, the `sealed` list (segments on S3) should already be dispatched as S3 requests and the data should be
   streaming back to the Query Service. At the moment, the evaluation happens on the Query Service and in the future can 
   be moved to Lambda.
7. There is one data stream per dataExpr per segment headed towards the Query Service node that received the query
   and is responsible for federating the responses by K-way merge sorting these `N` streams into a single stream
   which is then fed into the main expression evaluator. 
8. At this point, we are ready to stream the response (output of the main evaluator). Currently, we stream sealed aggregations
   followed by unsealed aggregations. This means time to first datapoint is a function of how quickly we can scan sealed segments,
   which isn't ideal.
9. (TBD) To remedy this, we need to introduce data expr level caching for sealed segments, so that only the first query
   takes the hit but future queries are faster. That said, for the first query, note that we are not waiting for all sealed segments
   to come back, and so time to first data point == time to first data point from the first sealed segment. 
10. (TBD) We also need to experiment to see how much lambda can help here.
