package org.apache.rocketmq.client.consumer.mytest;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

/**
 * @Description: [一句话描述该类的作用]
 * @Author: zhuyihao
 * @CreateDate: 2019/9/8 14:39
 * @Version: [v1.0]
 */
public class ConsumerPushTest {

    public static void main(String[] args) {
        DefaultMQPushConsumer consumer  = new DefaultMQPushConsumer();
        consumer.setNamesrvAddr("localhost:9876");
        consumer.setConsumerGroup("broker");
        try {

            consumer.subscribe("TopicTest", "push");
            consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);

            consumer.registerMessageListener(new MessageListenerConcurrently() {
                @Override
                public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                    for(Message item : msgs) {
                        System.out.println(new String(item.getBody()));
                    }
                    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                }
            });

            consumer.start();

            Thread.sleep(5000);

            consumer.suspend();


        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
