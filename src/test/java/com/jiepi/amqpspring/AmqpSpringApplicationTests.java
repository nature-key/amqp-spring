package com.jiepi.amqpspring;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.amqp.AmqpException;
import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.HashMap;

@RunWith(SpringRunner.class)
@SpringBootTest
public class AmqpSpringApplicationTests {

    @Autowired
    private RabbitAdmin rabbitAdmin;

    @Test
    public void contextLoads() {

        rabbitAdmin.declareExchange(new DirectExchange("test.direct", false, false));

        rabbitAdmin.declareExchange(new TopicExchange("test.topic", false, false));

        rabbitAdmin.declareExchange(new FanoutExchange("test.fanout", false, false));

        rabbitAdmin.declareQueue(new Queue("test.direct.queue", false));

        rabbitAdmin.declareQueue(new Queue("test.topic.queue", false));

        rabbitAdmin.declareQueue(new Queue("test.fanout,queue", false));


        rabbitAdmin.declareBinding(new Binding("test.direct.queue", Binding.DestinationType.QUEUE, "test.direct", "direct", new HashMap<>()));
//
//		rabbitAdmin.declareBinding(new Binding("test.topic.queue",Binding.DestinationType.QUEUE,"test.topic","topic",new HashMap<>()));
//
//
        rabbitAdmin.declareBinding(BindingBuilder.bind(new Queue("test.topic.queue"))
                .to(new TopicExchange("test.topic", false, false))
                .with("user.#"));
//
//
//
//		rabbitAdmin.declareBinding(BindingBuilder.bind(new Queue("test.fanout.queue"))
//		.to(new FanoutExchange("test.fanout",false,false))
//		);
//
//
        rabbitAdmin.purgeQueue("test.topic.queue", false);


    }


    @Autowired
    private RabbitTemplate rabbitTemplate;


    @Test
    public void test1() {
        MessageProperties messageProperties = new MessageProperties();
        messageProperties.getHeaders().put("test1", "test1");
        messageProperties.getHeaders().put("test2", "test2");
        Message message = new Message("HELLO WORD".getBytes(), messageProperties);
        rabbitTemplate.convertAndSend("topic001", "spring.save", message, new MessagePostProcessor() {
            @Override
            public Message postProcessMessage(Message message) throws AmqpException {
                System.out.println("-----------" + message + "--------------");
                message.getMessageProperties().getHeaders().put("test1", "test");
                message.getMessageProperties().getHeaders().put("test3", "test3");
                return message;
            }
        });
    }

    @Test
    public void test2() {
        MessageProperties messageProperties = new MessageProperties();
        messageProperties.getHeaders().put("test1", "test1");
        messageProperties.getHeaders().put("test2", "test2");
        Message message = new Message("HELLO WORD rabbit".getBytes(), messageProperties);

        rabbitTemplate.send("topic002", "rabbit.r", message);
        rabbitTemplate.convertAndSend("topic001", "spring.abc", "tipoc001_message_spring.abc");
        rabbitTemplate.convertAndSend("topic001", "spring.ab", "tipoc001_message_spring.ab");
        rabbitTemplate.convertAndSend("topic002", "rabbit.a", "tipoc001_message_rabbit");
    }

}
