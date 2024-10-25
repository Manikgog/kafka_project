package ru.t1.service;

import net.datafaker.Faker;
import org.springframework.stereotype.Component;
import ru.t1.dto.ClientDto;

import java.util.ArrayList;
import java.util.List;

@Component
public class ClientsGenerator {

    private final Faker faker = new Faker();

    public List<ClientDto> generate(Long numberOfClients) {
        List<ClientDto> clients = new ArrayList<>();
        for (int i = 0; i < numberOfClients; i++) {
            ClientDto client = new ClientDto(
                    faker.name().firstName(),
                    faker.name().nameWithMiddle(),
                    faker.name().lastName()
            );
            clients.add(client);
        }
        return clients;
    }

}
