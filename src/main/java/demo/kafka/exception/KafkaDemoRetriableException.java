package demo.kafka.exception;

public class KafkaDemoRetriableException extends RuntimeException implements Retryable {
    public KafkaDemoRetriableException(Throwable cause) {
        super(cause);
    }
}
