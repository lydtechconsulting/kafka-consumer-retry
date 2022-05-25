package demo.kafka.service;

import java.util.UUID;

import demo.kafka.exception.KafkaDemoException;
import demo.kafka.exception.KafkaDemoRetriableException;
import demo.kafka.properties.KafkaDemoProperties;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestTemplate;

@Service
@Slf4j
@RequiredArgsConstructor
public class DemoRetryService {
    private final String REQUEST_ID_PREFIX_STRING = "requestId: ";
    private final KafkaDemoProperties properties;
    private final KafkaTemplate kafkaTemplate;

    public void process(String payload) {
        callThirdparty(payload);
        sendMessage(payload);
    }

    public String extractRequestIdFromEventPayload(String payload) {
        return payload.substring(payload.lastIndexOf(REQUEST_ID_PREFIX_STRING) + REQUEST_ID_PREFIX_STRING.length());
    }

    private void callThirdparty(String payload) {
        RestTemplate restTemplate = new RestTemplate();
        try {
            String requestId = extractRequestIdFromEventPayload(payload);
            ResponseEntity<String> response = restTemplate.getForEntity(properties.getThirdpartyEndpoint() + "/" + requestId, String.class);
            if (response.getStatusCodeValue() != 200) {
                throw new RuntimeException("error " + response.getStatusCodeValue());
            }
            return;

        } catch (HttpServerErrorException e) { // HttpServerErrorException – in case of HTTP status 5xx
            log.error("Error calling thirdparty api, returned an (" + e.getClass().getName() + ") with an error code of " + e.getRawStatusCode(), e);   // e.getRawStatusCode()
            throw new KafkaDemoRetriableException(e);
        } catch (ResourceAccessException e) { // ResourceAccessException – in case of resource access exceptions
            log.error("Error calling thirdparty api, returned an (" + e.getClass().getName() + ")", e);
            throw new KafkaDemoRetriableException(e);
        } catch (Exception e) {
            log.error("Error calling thirdparty api, returned an (" + e.getClass().getName() + ")", e);
            throw new KafkaDemoException(e);
        }
    }

    private SendResult sendMessage(String originalPayload) {
        try {
            String payload = "eventId: " + UUID.randomUUID() + ", instanceId: "+properties.getInstanceId()+", payload: " + originalPayload;
            final ProducerRecord<String, String> record =
                    new ProducerRecord<>(properties.getOutboundTopic(), payload);

            final SendResult result = (SendResult) kafkaTemplate.send(record).get();
            final RecordMetadata metadata = result.getRecordMetadata();

            log.debug(String.format("Sent record(key=%s value=%s) meta(topic=%s, partition=%d, offset=%d)",
                    record.key(), record.value(), metadata.topic(), metadata.partition(), metadata.offset()));

            return result;
        } catch (Exception e) {
            log.error("Error sending message to topic " + properties.getOutboundTopic(), e);
            throw new KafkaDemoException(e);
        }
    }
}
