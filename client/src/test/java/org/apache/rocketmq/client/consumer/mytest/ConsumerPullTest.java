package org.apache.rocketmq.client.consumer.mytest;

import com.alibaba.fastjson.JSON;
import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.MessageQueueListener;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.common.message.MessageQueue;

import java.util.Set;

/**
 * @Description: [一句话描述该类的作用]
 * @Author: zhuyihao
 * @CreateDate: 2019/9/8 14:35
 * @Version: [v1.0]
 */
public class ConsumerPullTest {

    public static void main(String[] args) {
        DefaultMQPullConsumer consumer = new DefaultMQPullConsumer();
        consumer.setNamesrvAddr("localhost:9876");
        consumer.setConsumerGroup("broker");
        consumer.setConsumerPullTimeoutMillis(1000000000);

        try {

            consumer.start();

            Set<MessageQueue> messageQueues = consumer.fetchSubscribeMessageQueues("TopicTest");
            for (MessageQueue mq : messageQueues) {
                System.out.println(mq.getTopic());

                PullResult pullResult = consumer.pullBlockIfNotFound(mq, null, 0, 32);

                System.out.println(JSON.toJSONString(pullResult));
            }

            consumer.registerMessageQueueListener("", new MessageQueueListener() {
                @Override
                public void messageQueueChanged(String topic, Set<MessageQueue> mqAll, Set<MessageQueue> mqDivided) {
                    System.out.println("message queue changed");
                }
            });



        } catch (Exception e) {
            e.printStackTrace();
        }
    }



}
