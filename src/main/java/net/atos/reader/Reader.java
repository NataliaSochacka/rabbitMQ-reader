package net.atos.reader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class Reader {

    @Value("${QUEUE}")
    public String QUEUE;

    Logger logger = LoggerFactory.getLogger(Reader.class);

    @RabbitListener(queues = "${QUEUE}")
    public void readMessageFromQueue(Message msg){

        logger.info("Received: " + msg.getMessageID() + ", " + msg.getMessage());
    }
}
