# Kafka Consumer Retry Project

Spring Boot application demonstrating Kafka consumer retry.

This repo accompanies the article [Kafka Consumer Retry](https://medium.com/lydtech-consulting/kafka-consumer-retry-646aa5aad2e4).

## Integration Tests

Run integration tests with `mvn clean test`

The tests demonstrate stateless and stateful retry behaviour.  They use an embedded Kafka broker and a wiremock to
represent a third party service.  This call to the third party service simulates transient errors that can be successful
on retry.

## Component Tests

The tests demonstrate stateless and stateful retry behaviour.  They use a real dockerised Kafka broker and a dockerised
wiremock to represent a third party service.  This call to the third party service simulates transient errors that can be 
successful on retry.  Two instances of the service are also running in docker containers.

Build Spring Boot application jar:
```
mvn clean install
```

Build Docker container:
```
docker build -t ct/kafka-consumer-retry:latest .
```

Run tests:
```
mvn test -Pcomponent
```

Run tests leaving containers up:
```
mvn test -Pcomponent -Dcontainers.stayup
```

Manual clean up (if left containers up):
```
docker rm -f $(docker ps -aq)
```

### Test Overview

There are two tests which demonstrate stateless and stateful retry.

In both cases the Test sends an event to a topic, which the service consumes.

The wiremock is programmed to return a 503 (Service Unavailable) for the first 3 calls, and then to return a 200 (Success).

Two instances of the service are started (in docker containers).

The service retries, with a 4 second pause between each attempt.

The consumer poll is configured to timeout after 10 seconds.

In the case of the stateless test the consumer is configured for stateless retry, and for the stateful test the consumer
is configured for stateful retry.

### Stateless test

The Test sends an event to the service.

The wiremock returns 503s until the retry 4th attempt when the call succeeds.

The stateless retry happens within the poll, so the poll times out after the configured 10 seconds.  
 
The Broker believes the consumer may have died, removes it from the consumer group, re-balances the consumer group, and 
the event is re-delivered to the new consumer.

Both the first and second instance successfully process the event and publish a resulting event.

The logging shows the Test polling for the resulting events produced by the service.  It receives the two resulting 
events.  Note that both resulting events have unique eventIds, so downstream consumers of these events will not know 
they are duplicates.  The instanceId shows that they originated from each of the two service instances.  The payload has 
the original eventId of the consumed event, showing that these are the same event, processed twice.

```
10:34:38.262 INFO  d.k.c.RetryComponentTest - Stateless retry - sent event: f3b0bfae-8967-4b67-a206-38d86ea25535
10:34:40.802 INFO  d.k.c.RetryComponentTest - Stateless retry [1]- test received count: 0
10:34:42.315 INFO  d.k.c.RetryComponentTest - Stateless retry [2]- test received count: 0
10:34:43.824 INFO  d.k.c.RetryComponentTest - Stateless retry [3]- test received count: 0
10:34:45.332 INFO  d.k.c.RetryComponentTest - Stateless retry [4]- test received count: 0
10:34:46.840 INFO  d.k.c.RetryComponentTest - Stateless retry [5]- test received count: 0
10:34:48.347 INFO  d.k.c.RetryComponentTest - Stateless retry [6]- test received count: 0
10:34:49.363 INFO  d.k.c.RetryComponentTest - Stateless retry - received: eventId: 02810b9a-97fe-4133-a726-d7b262ac4f6f, instanceId: b017c70e-1ef9-4506-b1d9-35167a02cf32, payload: f3b0bfae-8967-4b67-a206-38d86ea25535
10:34:49.363 INFO  d.k.c.RetryComponentTest - Stateless retry [7]- test received count: 1
10:34:50.376 INFO  d.k.c.RetryComponentTest - Stateless retry - received: eventId: 5145c662-ba2e-4e1d-85a6-adbb57df9e5e, instanceId: 74be0404-3321-4b40-85fb-b44497f035f9, payload: f3b0bfae-8967-4b67-a206-38d86ea25535
10:34:50.376 INFO  d.k.c.RetryComponentTest - Stateless retry [8]- test received count: 2
10:34:51.884 INFO  d.k.c.RetryComponentTest - Stateless retry [9]- test received count: 2
10:34:53.393 INFO  d.k.c.RetryComponentTest - Stateless retry [10]- test received count: 2
```

### Stateful Retry

The Test sends an event to the service.

The wiremock returns 503s until the retry 4th attempt when the call succeeds.

The stateful retry re-polls the event from the broker, so the poll time out of 10 seconds is never exceeded as the 
retry period is only 4 seconds.

Only the first instance successfully processes the event and publishes a resulting event, as the original event is never
re-delivered.

The logging shows the Test polling for the resulting events produced by the service.  It receives the resulting event.

```
10:45:52.810 INFO  d.k.c.RetryComponentTest - Stateful retry - sent event: 89b7f94e-5a78-4c4d-b328-872d1212521e
10:45:55.371 INFO  d.k.c.RetryComponentTest - Stateful retry [1]- test received count: 0
10:45:56.889 INFO  d.k.c.RetryComponentTest - Stateful retry [2]- test received count: 0
10:45:58.398 INFO  d.k.c.RetryComponentTest - Stateful retry [3]- test received count: 0
10:45:59.909 INFO  d.k.c.RetryComponentTest - Stateful retry [4]- test received count: 0
10:46:01.418 INFO  d.k.c.RetryComponentTest - Stateful retry [5]- test received count: 0
10:46:02.921 INFO  d.k.c.RetryComponentTest - Stateful retry [6]- test received count: 0
10:46:04.429 INFO  d.k.c.RetryComponentTest - Stateful retry [7]- test received count: 0
10:46:05.441 INFO  d.k.c.RetryComponentTest - Stateful retry - received: eventId: 22c6a399-661e-4afa-9338-ab0608b7cd75, instanceId: b017c70e-1ef9-4506-b1d9-35167a02cf32, payload: 89b7f94e-5a78-4c4d-b328-872d1212521e
10:46:05.441 INFO  d.k.c.RetryComponentTest - Stateful retry [8]- test received count: 1
10:46:06.949 INFO  d.k.c.RetryComponentTest - Stateful retry [9]- test received count: 1
10:46:08.458 INFO  d.k.c.RetryComponentTest - Stateful retry [10]- test received count: 1
```
