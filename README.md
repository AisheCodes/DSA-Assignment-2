# DSA612S Distributed Food Delivery Platform

A deliberately small Ballerina project framework for the assignment.

## Structure

```text
client/
  Ballerina.toml
  Dependencies.toml
  client.bal
  pb.bal

service/
  Ballerina.toml
  Dependencies.toml
  server.bal
  pb.bal

proto/
  food_delivery.proto

web/
  index.html
```

## Run the service

```text
cd service
bal build
bal run
```

The service listens on `http://localhost:9090`.

## Run the client

Keep the service running, then in another terminal:

```text
cd client
bal build
bal run
```

## Protobuf/gRPC

The `.proto` file is included as a starting point. Ballerina can generate a
stub from it with the gRPC tool; the generated output normally uses a
`*_pb.bal` name.

```text
bal grpc --input ../proto/food_delivery.proto --output .
```

This framework does not depend on the generated gRPC code yet; the initial
REST API is intentionally kept simple.

## Assignment expansion

Add the remaining assignment concerns incrementally:

1. Kafka producers/consumers.
2. Persistent database storage.
3. Order transition validation.
4. Driver and delivery logic.
5. Notifications.
6. Admin/reporting.
7. Docker Compose.
