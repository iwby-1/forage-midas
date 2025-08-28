package com.jpmc.midascore.component;

import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.repository.UserRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import java.util.Optional;

@Component
public class BalanceQuerier {

    @Autowired
    private UserRepository userRepository;

    public float getBalance(String userName) {
        Optional<UserRecord> userRecordOptional = userRepository.findByName(userName);
        if (userRecordOptional.isPresent()) {
            return userRecordOptional.get().getBalance();
        }
        return 0.0f;
    }
}