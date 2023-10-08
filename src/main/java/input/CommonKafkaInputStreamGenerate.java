package input;

import common.GeneralParameters;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Properties;

/**
 * 抽取出来的获取kafka的两个输入数据源的类
 */
public class CommonKafkaInputStreamGenerate {

    //设置消费者组名
    private final static String consumerGroupName = "test_group_1";

   public static DataStreamSource<String> getFirstInputStream(StreamExecutionEnvironment env) {
        //设置kafka数据源，并设置相关参数
        Properties kafkaProperties = new Properties();
        kafkaProperties.setProperty("bootstrap.servers", GeneralParameters.KAFKA_BROKER_LIST);
        kafkaProperties.setProperty("group.id", consumerGroupName);

        //构建kafka消费者
        FlinkKafkaConsumer011<String> myKafkaConsumer = new FlinkKafkaConsumer011<>(GeneralParameters.FIRST_KAFKA_REAL_TIME_TEST_TOPIC, new SimpleStringSchema(), kafkaProperties);

        //设置从最早的记录开始读取
//        myKafkaConsumer.setStartFromEarliest();
        //设置从最新的记录开始读取
        myKafkaConsumer.setStartFromLatest();

        //构建kafka数据源(正式数据源)
        DataStreamSource<String> inputStream = env.addSource(myKafkaConsumer).setParallelism(GeneralParameters.KAFKA_R_TOPIC_PARTITIONS_NUM);

        return inputStream;
    }

    public static DataStreamSource<String> getSecondInputStream(StreamExecutionEnvironment env) {
        //设置kafka数据源，并设置相关参数
        Properties kafkaProperties = new Properties();
        kafkaProperties.setProperty("bootstrap.servers", GeneralParameters.KAFKA_BROKER_LIST);
        kafkaProperties.setProperty("group.id", consumerGroupName);

        //构建kafka消费者
        FlinkKafkaConsumer011<String> myKafkaConsumer = new FlinkKafkaConsumer011<>(GeneralParameters.SECOND_KAFKA_REAL_TIME_TEST_TOPIC, new SimpleStringSchema(), kafkaProperties);

        //设置从最早的记录开始读取
//        myKafkaConsumer.setStartFromEarliest();
        //设置从最新的记录开始读取
        myKafkaConsumer.setStartFromLatest();

        //构建kafka数据源(正式数据源)
        DataStreamSource<String> inputStream = env.addSource(myKafkaConsumer).setParallelism(GeneralParameters.KAFKA_S_TOPIC_PARTITIONS_NUM);

        return inputStream;
    }
}
