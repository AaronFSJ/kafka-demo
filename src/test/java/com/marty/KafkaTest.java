package com.marty;//package com.marty.config;
//
//import com.marty.listener.Listener;
//import javafx.application.Application;
//import org.junit.Test;
//import org.junit.runner.RunWith;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.kafka.controller.KafkaTemplate;
//
//import java.util.concurrent.TimeUnit;
//
//import static org.junit.Assert.assertTrue;
//
///**
// * @Author: Aaron
// * @Descprition:
// * @Date: Create in 2017/12/20 09:41
// * @Modyfied By:
// */
//
//
//public class KafkaTest {
//    @Autowired
//    private Listener listener;
//
//    @Autowired
//    private KafkaTemplate<Integer, String> template;
//
//    @Test
//    public void testSimple() throws Exception {
//        template.send("annotated1", 0, "foo");
//        template.flush();
//        assertTrue(this.listener.getLatch1().await(10, TimeUnit.SECONDS));
//    }
//}
