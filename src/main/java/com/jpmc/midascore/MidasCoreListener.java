package com.jpmc.midascore;

import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.foundation.Transaction;
import com.jpmc.midascore.foundation.Incentive;
import com.jpmc.midascore.repository.UserRepository;
import com.jpmc.midascore.repository.TransactionRecordRepository;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.client.RestTemplate;
import org.springframework.beans.factory.annotation.Value;
import java.math.BigDecimal;
import java.util.Optional;

@Component
public class MidasCoreListener {

    private final UserRepository userRepository;
    private final TransactionRecordRepository transactionRecordRepository;
    private final RestTemplate restTemplate;

    @Value("${incentive.api.url:http://localhost:8080/incentive}")
    private String incentiveApiUrl;

    public MidasCoreListener(UserRepository userRepository, TransactionRecordRepository transactionRecordRepository, RestTemplate restTemplate) {
        this.userRepository = userRepository;
        this.transactionRecordRepository = transactionRecordRepository;
        this.restTemplate = restTemplate;
    }

    @KafkaListener(topics = "transactions-topic", groupId = "midas-core-group")
    @Transactional
    public void listen(Transaction transaction) {
        Optional<UserRecord> senderOpt = userRepository.findById(transaction.getSenderId());
        Optional<UserRecord> recipientOpt = userRepository.findById(transaction.getRecipientId());

        if (senderOpt.isEmpty() || recipientOpt.isEmpty()) {
            System.err.println("Invalid sender or recipient. Transaction discarded.");
            return;
        }

        UserRecord sender = senderOpt.get();
        UserRecord recipient = recipientOpt.get();

        if (sender.getBalance() < transaction.getAmount()) {
            System.err.println("Insufficient funds. Transaction discarded.");
            return;
        }

        Incentive incentive = restTemplate.postForObject(
                incentiveApiUrl,
                transaction,
                Incentive.class
        );

        BigDecimal senderBalance = BigDecimal.valueOf(sender.getBalance());
        BigDecimal recipientBalance = BigDecimal.valueOf(recipient.getBalance());
        BigDecimal amount = BigDecimal.valueOf(transaction.getAmount());
        BigDecimal incentiveAmount = BigDecimal.valueOf(incentive.getAmount());

        sender.setBalance(senderBalance.subtract(amount).floatValue());
        recipient.setBalance(recipientBalance.add(amount).add(incentiveAmount).floatValue());

        userRepository.save(sender);
        userRepository.save(recipient);

        TransactionRecord record = new TransactionRecord(sender, recipient, transaction.getAmount());
        record.setIncentive(incentive.getAmount());
        transactionRecordRepository.save(record);
    }
}