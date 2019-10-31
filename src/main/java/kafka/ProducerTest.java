package kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.utils.Utils;
import util.DateFmt;

import java.util.Properties;
import java.util.Random;

/**
 * 模拟kafka生产者生成假数据发送
 */
public class ProducerTest extends Thread{

    private static KafkaProducer<Integer,String> producer;
    private static String topic;
    private static Properties props = new Properties();

    public  ProducerTest(String topic){
        this.topic = topic;
        //表是要发送字符串消息
        //props.put("bootstrap.servers","hdp-2:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        //kafka服务的集群
        props.put("bootstrap.servers","hdp-1:9092,hdp-2:9092,hdp-3:9092");
        producer = new KafkaProducer<Integer, String>(props);
    }

    public void run() {
        Random random = new Random();
        String[] cell_num = { "29448-37062", "29448-51331", "29448-51331", "29448-51333", "29448-51343" };
        // 正常0； 掉话1(信号断断续续)； 断话2(完全断开)
        String[] drop_num = { "0", "1", "2" };
        int i = 0;
        while (true) {
            i++;
            String testStr = String.format("%06d", random.nextInt(10) + 1);

            // messageStr: 2494 29448-000003 2016-01-05 10:25:17 1

            String messageStr = i + "\t" + ("29448-" + testStr) + "\t" + DateFmt.getCountDate(null, DateFmt.date_long)
                    + "\t" + drop_num[random.nextInt(drop_num.length)];
            System.out.println("product:" + messageStr);
            //生产者向外发送的数据
            producer.send(new ProducerRecord<Integer, String>(topic,messageStr));
            Utils.sleep(1000);
        }
    }

    public static void main(String[] args) {
        ProducerTest producerTest = new ProducerTest("mylog_cmcc");
        producerTest.start();
    }
}
