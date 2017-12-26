//package com.marty;
//
//import com.marty.listener.Listener;
//import org.apache.kafka.clients.consumer.ConsumerRecord;
//import org.junit.Test;
//import org.junit.runner.RunWith;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.boot.test.context.SpringBootTest;
//import org.springframework.kafka.annotation.KafkaListener;
//import org.springframework.kafka.core.KafkaTemplate;
//import org.springframework.test.context.junit4.SpringRunner;
//
//import java.util.concurrent.TimeUnit;
//
//import static junit.framework.TestCase.assertTrue;
//
//@RunWith(SpringRunner.class)
//@SpringBootTest
//public class KafkaDemoApplicationTests {
//
//	@Autowired
//    private Listener listener;
//
//    @Autowired
//    private KafkaTemplate<Integer, String> template;
////
////    @Test
////    public void testSimple() throws Exception {
////        template.send("annotated1", 0, "foo");
////        template.flush();
////        assertTrue(this.listener.getLatch1().await(10, TimeUnit.SECONDS));
////    }
//
////	@Test
////	public void send() {
////		template.send("annotated1", 0, "foo");
////        template.flush();
////		try {
////			assertTrue(this.listener.getLatch1().await(10, TimeUnit.SECONDS));
////		} catch (InterruptedException e) {
////			e.printStackTrace();
////		}
////	}
////
////	@KafkaListener(topics = "annotated1")
////	public void listen(ConsumerRecord<?, ?> cr) throws Exception {
////		System.out.println(cr.toString());
////	}
//
//}
