package repro;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;
import org.springframework.amqp.AmqpRejectAndDontRequeueException;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.amqp.rabbit.listener.api.ChannelAwareMessageListener;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.util.backoff.FixedBackOff;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * A listener container with a transaction manager (=> channelTransacted consumer channel) invokes a listener that
 * opens a PROPAGATION_REQUIRES_NEW transaction and sends a message with a channelTransacted RabbitTemplate.
 * Expected: the consumer channel is untouched by the inner transaction.
 * Actual: the inner transaction commit closes the consumer's channel.
 */
@Testcontainers
class ConsumerChannelClosedByRequiresNewTest {

    @Container
    static final RabbitMQContainer RABBIT = new RabbitMQContainer("rabbitmq:4");

    @Test
    void control_templateNotChannelTransacted_consumerChannelSurvives() throws Exception {
        assertConsumerChannelSurvivesInnerTransaction(false);
    }

    @Test
    void repro_templateChannelTransacted_consumerChannelSurvives() throws Exception {
        assertConsumerChannelSurvivesInnerTransaction(true);
    }

    private void assertConsumerChannelSurvivesInnerTransaction(boolean templateChannelTransacted) throws Exception {
        CachingConnectionFactory cf = new CachingConnectionFactory(RABBIT.getHost(), RABBIT.getAmqpPort());
        cf.setChannelCacheSize(1);

        DataSourceTransactionManager tm = new DataSourceTransactionManager(
                new DriverManagerDataSource("jdbc:h2:mem:repro" + templateChannelTransacted));
        TransactionTemplate requiresNew = new TransactionTemplate(tm);
        requiresNew.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);

        RabbitTemplate template = new RabbitTemplate(cf);
        template.setChannelTransacted(templateChannelTransacted);

        String queue = "in-" + templateChannelTransacted;
        new RabbitAdmin(cf).declareQueue(new Queue(queue));

        List<Boolean> channelOpenAfterInnerTx = new CopyOnWriteArrayList<>();
        AtomicInteger deliveries = new AtomicInteger();

        SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(cf);
        container.setQueueNames(queue);
        container.setTransactionManager(tm);
        container.setChannelTransacted(true);
        container.setRecoveryBackOff(new FixedBackOff(200, FixedBackOff.UNLIMITED_ATTEMPTS));
        container.setMessageListener((ChannelAwareMessageListener) (message, channel) -> {
            requiresNew.executeWithoutResult(s -> template.convertAndSend("out-" + queue, "payload"));
            channelOpenAfterInnerTx.add(channel.isOpen());
            deliveries.incrementAndGet();
        });
        container.start();
        try {
            template.convertAndSend(queue, "trigger");
            Thread.sleep(3000);
        } finally {
            container.stop();
            cf.destroy();
        }

        assertEquals(1, deliveries.get(), "message must be delivered exactly once, but was delivered again");
        assertTrue(channelOpenAfterInnerTx.get(0), "consumer channel was closed by the inner transaction");
    }

    /**
     * Same setup, default channel cache (so the channel is not closed, only shared). The outer transaction sends a
     * message and then rolls back; the inner REQUIRES_NEW transaction sends another message and commits.
     * Expected: only the inner message is visible. Actual: the inner commit also commits the outer's send.
     */
    @Test
    void repro_innerCommitAlsoCommitsOuterTransactionsWork() throws Exception {
        CachingConnectionFactory cf = new CachingConnectionFactory(RABBIT.getHost(), RABBIT.getAmqpPort());
        DataSourceTransactionManager tm = new DataSourceTransactionManager(new DriverManagerDataSource("jdbc:h2:mem:shared"));
        TransactionTemplate requiresNew = new TransactionTemplate(tm);
        requiresNew.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);

        RabbitTemplate template = new RabbitTemplate(cf);
        template.setChannelTransacted(true);

        RabbitAdmin admin = new RabbitAdmin(cf);
        for (String q : List.of("in", "outer", "inner")) {
            admin.declareQueue(new Queue(q));
        }

        SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(cf);
        container.setQueueNames("in");
        container.setTransactionManager(tm);
        container.setChannelTransacted(true);
        container.setMessageListener((ChannelAwareMessageListener) (message, channel) -> {
            template.convertAndSend("outer", "sent in the outer transaction, which rolls back");
            requiresNew.executeWithoutResult(s -> template.convertAndSend("inner", "sent in the inner transaction"));
            throw new AmqpRejectAndDontRequeueException("roll back the outer transaction");
        });
        container.start();
        try {
            template.convertAndSend("in", "trigger");
            Thread.sleep(3000);
        } finally {
            container.stop();
        }

        RabbitTemplate nonTransacted = new RabbitTemplate(cf);
        assertEquals("sent in the inner transaction", nonTransacted.receiveAndConvert("inner"));
        assertEquals(null, nonTransacted.receiveAndConvert("outer"),
                "message sent in the rolled back outer transaction was committed by the inner transaction");
        cf.destroy();
    }
}
