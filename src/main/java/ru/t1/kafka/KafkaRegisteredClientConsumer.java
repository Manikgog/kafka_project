package ru.t1.kafka;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;
import ru.t1.entity.Client;

import java.util.List;

@Slf4j
@RequiredArgsConstructor
@Component
@KafkaListener(topics = "${t1.kafka.topic.client_id_registered}")
public class KafkaRegisteredClientConsumer {

    @KafkaListener(id = "${t1.kafka.consumer.group-id}",
            topics = "${t1.kafka.topic.client_id_registered}",
            containerFactory = "kafkaListenerContainerFactory1")
    public void listener(@Payload List<Client> clients,
                         Acknowledgment ack,
                         @Header(KafkaHeaders.RECEIVED_KEY) String key) {

        if (clients.isEmpty()) {
            log.warn("Пустой пакет сообщений получен, обработка пропущена");
            return;
        }

        log.debug("Client id consumer: Обработка новых сообщений");
        try {
            clients.forEach(client -> {
                log.info("Клиент " + client.getFirstName() + " " + client.getLastName() + " зарегистрирован под id=" + client.getId());
            });
        } finally {
            /*
            Вызывается, когда запись или партия, для которой было создано подтверждение, обработана.
            Вызов этого метода подразумевает, что все предыдущие сообщения в разделе уже обработаны.
             */
            ack.acknowledge();
        }

        log.debug("Registered clients consumer: записи обработаны");
    }
}
