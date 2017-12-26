package com.marty.listener;

import com.marty.entity.User;
import com.marty.util.BeanUtil;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaListener;

import java.util.concurrent.CountDownLatch;

/**
 * @Author: Aaron
 * @Descprition:
 * @Date: Create in 2017/12/20 09:44
 * @Modyfied By:
 */
@Configuration
public class Listener {

    protected final Logger logger = LoggerFactory.getLogger(this.getClass());


    @KafkaListener(topics = {"user"})
    public void listen1(ConsumerRecord<?, ?> record) {
        System.out.println("进入消费者user");
        logger.info(record.toString());
        logger.info("user kafka的topic: " + record.topic());
        //因为消费者用ByteArrayDeserialize反序列化，value实际上是byte[]
        byte[] b = (byte[]) record.value();
        if(b !=null){
            User user = (User) BeanUtil.BytesToObject(b);
            logger.info("====用户名：{}===",user.getName());
        }

        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @KafkaListener(topics = {"test"})
    public void listen2(ConsumerRecord<?, ?> record) {
        System.out.println("进入消费者test");
        logger.info(record.toString());
        logger.info("test kafka的topic: " + record.topic());
        //因为消费者用ByteArrayDeserialize反序列化，所以接收的时候任何对象都要转成byte[]
        byte[] b = (byte[]) record.value();
        if(b !=null){
           logger.info("接收到的test数据为：{}",new String(b));
        }

        logger.info("kafka的value: " + record.value().toString());
    }


}
