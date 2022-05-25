package demo.kafka.integration;

import java.util.concurrent.TimeUnit;

import demo.kafka.consumer.StatefulRetryConsumer;
import lombok.extern.slf4j.Slf4j;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.TestPropertySource;

import static com.github.tomakehurst.wiremock.client.WireMock.exactly;
import static com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.verify;
import static org.hamcrest.Matchers.equalTo;

@Slf4j
@EmbeddedKafka(controlledShutdown = true, topics = "demo-stateful-retry-topic")
@TestPropertySource(properties="kafka.consumer.maxPollIntervalMs=10000")
public class KafkaStatefulRetryIntegrationTest extends IntegrationTestBase {

    private final static String STATEFUL_RETRY_TEST_TOPIC = "demo-stateful-retry-topic";

    @Autowired
    private StatefulRetryConsumer consumer;

    @Test
    public void testStatefulRetry() throws Exception {
        // prime the rest api mock to return service unavailable every time it's called
        stubWiremock("/api/kafkaretrydemo/" + requestId, 500, "Unavailable");

        sendMessage(STATEFUL_RETRY_TEST_TOPIC, requestId);

        Awaitility.await().atMost(30, TimeUnit.SECONDS).pollDelay(20, TimeUnit.SECONDS)
                .until(consumer.getCounter()::get, equalTo(5));
        verify(exactly(5), getRequestedFor(urlEqualTo("/api/kafkaretrydemo/" + requestId)));
    }
}
