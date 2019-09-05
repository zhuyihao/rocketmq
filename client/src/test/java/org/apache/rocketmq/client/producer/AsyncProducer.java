package org.apache.rocketmq.client.producer;

import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

/**
 * @Description: [一句话描述该类的作用]
 * https://www.jianshu.com/p/42330afbe53a
 * @Author: zhuyihao
 * @CreateDate: 2019/9/5 18:07
 * @Version: [v1.0]
 */
public class AsyncProducer {

    public static void main(String[] args) throws Exception {
        //Instantiate with a producer group name.
        DefaultMQProducer producer = new DefaultMQProducer("please_rename_unique_group_name");


        // Specify name server addresses.
        producer.setNamesrvAddr("localhost:9876");


        //防止代码在调试过程中的各种超时
        producer.setSendMsgTimeout(1000000000);
        producer.setRetryTimesWhenSendAsyncFailed(0);

        //Launch the instance.
        producer.start();


        for (int i = 0; i < 1; i++) {
            final int index = i;
            //Create a message instance, specifying topic, tag and message body.
            Message msg = new Message("TopicTest",
                    "TagA",
                    "OrderID188",
                    "Hello world".getBytes(RemotingHelper.DEFAULT_CHARSET));
            producer.send(msg, new SendCallback() {

                public void onSuccess(SendResult sendResult) {
                    System.out.printf("%-10d OK %s %n", index,
                            sendResult.getMsgId());
                }

                public void onException(Throwable e) {
                    System.out.printf("%-10d Exception %s %n", index, e);
                    e.printStackTrace();
                }
            });
        }

        Thread.sleep(10000000000L);
        //Shut down once the producer instance is not longer in use.
        producer.shutdown();
    }
}
