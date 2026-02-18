package com.jpmc.midascore;

import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Balance;
import com.jpmc.midascore.foundation.Transaction;
import com.jpmc.midascore.repository.TransactionRecordRepository;
import com.jpmc.midascore.repository.UserRepository;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

@Component
public class KafkaConsumer {
    private final UserRepository userRepository;
    private final TransactionRecordRepository transactionRepository;
    private final RestTemplate restTemplate;


    public KafkaConsumer(UserRepository userRepository, TransactionRecordRepository transactionRepository, RestTemplate restTemplate) {
        this.userRepository = userRepository;
        this.transactionRepository = transactionRepository;
        this.restTemplate = restTemplate;
    }

    @KafkaListener(topics = "${general.kafka-topic}", groupId = "midas-core-group")
    public void listen(Transaction transaction) {
        UserRecord sender = userRepository.findById(transaction.getSenderId());
        UserRecord recipient = userRepository.findById(transaction.getRecipientId());

        if (sender != null && recipient != null && sender.getBalance() >= transaction.getAmount()) {
            // 1. Call the Incentive API
            String url = "http://localhost:8080/incentive";
            // We use the Balance class you saw earlier to receive the { "amount": X } response
            Balance incentiveResponse = restTemplate.postForObject(url, transaction, Balance.class);
            float incentive = (incentiveResponse != null) ? incentiveResponse.getAmount() : 0f;

            // 2. Update Balances (Incentive only goes to recipient!)
            sender.setBalance(sender.getBalance() - transaction.getAmount());
            recipient.setBalance(recipient.getBalance() + transaction.getAmount() + incentive);

            // 3. Persist
            userRepository.save(sender);
            userRepository.save(recipient);
            transactionRepository.save(new TransactionRecord(sender, recipient, transaction.getAmount(), incentive));
        }
    }
}