package demo.kafka.integration;

import java.util.UUID;

import com.github.tomakehurst.wiremock.client.WireMock;
import demo.kafka.KafkaDemoConfiguration;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.contract.wiremock.AutoConfigureWireMock;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;

@Slf4j
@SpringBootTest(classes = { KafkaDemoConfiguration.class } )
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@AutoConfigureWireMock(port=0) // 0 is dynamic port which binds to the "wiremock.server.port" property
@ActiveProfiles("test")
public abstract class IntegrationTestBase {

    @Autowired
    private KafkaTemplate kafkaTemplate;

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    @Autowired
    private KafkaListenerEndpointRegistry registry;

    // Request Id for messages.
    protected String requestId;

    @BeforeEach
    public void setUp() {
        requestId = String.valueOf(System.currentTimeMillis());
        resetWiremock();

        // Wait until the partitions are assigned.
        registry.getListenerContainers().stream().forEach(container ->
                ContainerTestUtils.waitForAssignment(container, embeddedKafkaBroker.getPartitionsPerTopic()));
    }

    @AfterEach
    public void tearDown() {
        // Stop the consumer listener cleanly as otherwise it continues forever.
        registry.stop();
    }

    void resetWiremock() {
        log.debug("*** resetting Wiremock ***");
        WireMock.reset();
        WireMock.resetAllRequests();
        WireMock.resetAllScenarios();
        WireMock.resetToDefault();
    }

    void stubWiremock(String url, int httpStatusResponse, String body) {
        stubWiremock(url, httpStatusResponse, body, null, null, null);
    }

    void stubWiremock(String url, int httpStatusResponse, String body, String scenario, String initialState, String nextState) {
        if (scenario != null) {
            stubFor(get(urlEqualTo(url))
                    .inScenario(scenario)
                    .whenScenarioStateIs(initialState)
                    .willReturn(aResponse().withStatus(httpStatusResponse).withHeader("Content-Type", "text/plain").withBody(body))
                    .willSetStateTo(nextState));
        } else {
            stubFor(get(urlEqualTo(url))
                    .willReturn(aResponse().withStatus(httpStatusResponse).withHeader("Content-Type", "text/plain").withBody(body)));
        }
    }

    SendResult sendMessage(String topic, String requestId) throws Exception {
        String message = "eventId: " + UUID.randomUUID() + ", requestId: " + requestId;
        final ProducerRecord<Long, String> record =
                new ProducerRecord<>(topic, message);

        final SendResult result = (SendResult)kafkaTemplate.send(record).get();
        final RecordMetadata metadata = result.getRecordMetadata();

        log.debug(String.format("Sent record(key=%s value=%s) meta(topic=%s, partition=%d, offset=%d)",
                record.key(), record.value(), metadata.topic(), metadata.partition(), metadata.offset()));

        return result;
    }
}
