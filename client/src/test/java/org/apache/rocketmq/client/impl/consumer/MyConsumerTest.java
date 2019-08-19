package org.apache.rocketmq.client.impl.consumer;

import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.junit.Test;

import java.util.List;
import java.util.Set;

/**
 * @Description: [一句话描述该类的作用]
 * @Author: zhuyihao
 * @CreateDate: 2019/8/19 21:03
 * @Version: [v1.0]
 */
public class MyConsumerTest {



    @Test
    public void testPushMessage() throws Exception {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("CID_JODIE_1");
        consumer.subscribe("TopicTest111", "*");
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumer.setInstanceName("consumer1");
        consumer.setNamesrvAddr("127.0.0.1:9876");
        consumer.registerMessageListener(new MessageListenerConcurrently() {

            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                System.out.printf("%s Receive New Messages: %s %n", Thread.currentThread().getName(), msgs);
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        consumer.start();
    }


    @Test
    public void testPullMessage() throws Exception {
//        DefaultMQPullConsumer consumer = new DefaultMQPullConsumer("please_rename_unique_group_name_5");
//        consumer.setNamesrvAddr("127.0.0.1:9876");
//        consumer.setInstanceName("consumer");
//        consumer.start();
//
//        Set<MessageQueue> mqs = consumer.fetchSubscribeMessageQueues("TopicTest111");
//        for (MessageQueue mq : mqs) {
//            System.out.printf("Consume from the queue: %s%n", mq);
//            SINGLE_MQ:
//            while (true) {
//                try {
//                    PullResult pullResult =
//                            consumer.pullBlockIfNotFound(mq, null, getMessageQueueOffset(mq), 32);
//                    System.out.printf("%s%n", pullResult);
//                    putMessageQueueOffset(mq, pullResult.getNextBeginOffset());
//                    switch (pullResult.getPullStatus()) {
//                        case FOUND:
//                            System.out.println(pullResult.getMsgFoundList().get(0).toString());
//                            break;
//                        case NO_NEW_MSG:
//                            break SINGLE_MQ;
//                        case NO_MATCHED_MSG:
//                        case OFFSET_ILLEGAL:
//                            break;
//                        default:
//                            break;
//                    }
//                } catch (Exception e) {
//                    //TODO
//                }
//            }
//        }
//        consumer.shutdown();
    }


}
