package com.marty.controller;


import com.marty.entity.User;
import com.marty.util.BeanUtil;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.math.BigDecimal;


@RestController
@RequestMapping("/kafka")
public class CollectController {
    protected final Logger logger = LoggerFactory.getLogger(this.getClass());
    @Autowired
    private KafkaTemplate kafkaTemplate;

//    @RequestMapping(value = "/send", method = RequestMethod.GET)
//    public String sendKafka(HttpServletRequest request, HttpServletResponse response) {
//        String user = sendUser(request,response);
//        String test = sendTest(request,response);
//        return user+","+test;
//    }

    @RequestMapping(value = "/sendUser", method = RequestMethod.GET)
    public String sendUser(HttpServletRequest request, HttpServletResponse response){
        try {
            String message = request.getParameter("message");
            logger.info("user kafka的消息={}",message);
            User user = new User("1","Aaron",30,new BigDecimal(("20000")));
            //将对象转成byte数组
            byte[] u= BeanUtil.ObjectToBytes(user);
            /**
             * spring-kafka,可以实现producer的消息均发，但是如果producer中没有定义分区策略，也就是说程序采用默认的kafka.producer.DefaultPartitioner
             * 其核心思想就是对每个消息的key的hash值对partition数取模得到，所以如果key没有传入，则key为null,代码每次的hash值都一样，因此只发到了一个分区上，
             * 所以如果想要分送到不同的分区上，可以自己实现Partitioner，也可以指定每条消息发送的key,key值要根据实际情况发生变化，虽然不是均发，
             * 但是至少能保证不是每个消息都发送到同一个分区上
             */
            ProducerRecord<String,Object> pr = new ProducerRecord<String,Object>("user","user",u);
            for(int i =0;i<2;i++){
                kafkaTemplate.send(pr);
                logger.info("已经发送一个user");
                Thread.sleep(2000);
            }
            logger.info("发送user kafka成功.");
            return "发送user kafka成功";
        } catch (Exception e) {
            logger.error("发送user kafka失败", e);
            return "发送user kafka失败";
        }
    }

    /*
     * @Author: Aaron
     * @Description: 发送topic=test
     * @Date: 2017/12/26 09:33
     * @Param:
     * @url http://localhost:8080/kafka/sendTest?message=122434
     */
    @RequestMapping(value = "/sendTest", method = RequestMethod.GET)
    public String sendTest(HttpServletRequest request, HttpServletResponse response){
        try {
            String message = request.getParameter("message");
            logger.info("test kafka的消息={}",message);
            /*因为生产者用的是ByteArraySerializer发送消息，所以发送的时候任何对象都要转成byte[],
            如果用的是StringSerializer，且接收的是StringDeserializer那可以直接将String类型进行发送*/
            byte[] b = message.getBytes();
            kafkaTemplate.send("test", "key",b );

            logger.info("发送test kafka成功.");
            return "发送test kafka成功";
        } catch (Exception e) {
            logger.error("发送test kafka失败", e);
            return "发送test kafka失败";
        }
    }

    /**
     * spring boot方式启动的
     * @return
     */
    @RequestMapping(value = "/sendBySpringboot", method = RequestMethod.GET)
    public String sendBySpringboot(HttpServletRequest request, HttpServletResponse response){
        try {
            /*因为spring boot方式启动时用的是默认方式 ，即key和value都是用StringSerializer*/
            kafkaTemplate.send("myTopic", "foo1");
            logger.info("myTopic kafka的消息={}");

            logger.info("发送myTopic kafka成功.");
            return "发送myTopic kafka成功";
        } catch (Exception e) {
            logger.error("发送myTopic kafka失败", e);
            return "发送myTopic kafka失败";
        }
    }



}