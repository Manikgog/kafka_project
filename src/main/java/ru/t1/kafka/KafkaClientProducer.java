package ru.t1.kafka;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import ru.t1.dto.ClientDto;
import ru.t1.entity.Client;
import java.util.UUID;

@Slf4j
@RequiredArgsConstructor
public class KafkaClientProducer {

    private final KafkaTemplate<String, ClientDto> template;
    private final KafkaTemplate<String, Client> kafkaTemplate;

    public void send(String topic, Client client) {
        try {
            kafkaTemplate.send(topic, UUID.randomUUID().toString(), client).get();
            kafkaTemplate.flush();
        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
        }
    }

    public void sendTo(String topic, ClientDto o) {
        try {
            template.send(topic, UUID.randomUUID().toString(), o).get();
            template.flush();
        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
        }
    }

}
