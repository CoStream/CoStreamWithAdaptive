package experiment;

import base.CommonJoinUnionStream;
import base.CommonJoinUnionType;

import coModel.CoModelUnionStream;
import coModel.tools.sync.ZookeeperBasedSynchronizer;

import common.GeneralParameters;
import experiment.component.ComputeResultTupleLatencyMapFunction;
import experiment.component.MyCommonTestWatermarkPeriodicAssigner;
import experiment.component.ParallelAccumulateResultLatencyMapFunction;
import input.CommonKafkaInputStreamGenerate;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.log4j.Logger;
import tools.common.ParseExperimentParametersTool;

import java.io.Serializable;
import java.util.Optional;
import java.util.Properties;

/**
 * 将执行实验的流程封装成一个类，通过制定不同的Router执行不同的方法
 * 此实验为窗口为10分钟的等值连接
 */
public class ExperimentExecutorForSyntheticAndEqual implements Serializable {
    //用于标识是在进行哪个实验
    private String experimentModel;

    //保存程序的输入参数
    private String[] args;

    //获取logger对象用于日志记录
    private static final Logger logger = Logger.getLogger(ExperimentExecutorForSyntheticAndEqual.class.getName());

    /**
     * 构造器
     * @param experimentModel 指示是哪个程序的实验
     * @param args 程序的输入参数
     */
    public ExperimentExecutorForSyntheticAndEqual(String experimentModel, String[] args) {
        this.experimentModel = experimentModel;
        this.args = args;
    }


    /**
     * 实际进行实验
     * @throws Exception
     */
    public void startExperiment() throws Exception {

        logger.info(">>>开始执行ExperimentExecutorForSyntheticAndEqual实验");

        //将程序的输入参数上传到Zookeeper
        uploadExpConfigToZookeeper();

        //获取环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置事件时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //设置发送水位线的时间间隔,500ms
        env.getConfig().setAutoWatermarkInterval(500);
        //设置网络传输刷新时间(不同程序统一设定)
        env.setBufferTimeout(GeneralParameters.FLINK_OUTPUT_FLUSH_PERIOD);

        //用于指示范围连接的范围
        double r_surpass_s = Double.parseDouble(
                ParseExperimentParametersTool.parseExpParametersForName(args, ParseExperimentParametersTool.PARAM_NAME_OF_R_SURPASS_S));
        double r_behind_s = Double.parseDouble(
                ParseExperimentParametersTool.parseExpParametersForName(args, ParseExperimentParametersTool.PARAM_NAME_OF_R_BEHIND_S));

        System.out.println("范围连接种设置的范围为：r_surpass_s-" + r_surpass_s + ";r_behind_s-" + r_behind_s);
        System.out.println("程序的上下游算子之间元组刷新时间为：" + GeneralParameters.FLINK_OUTPUT_FLUSH_PERIOD);

        //构建数据源
        DataStream<Tuple4<String, String, String, String>> firstStream = CommonKafkaInputStreamGenerate.getFirstInputStream(env)
                .map(new MapStringToTuple4()).setParallelism(GeneralParameters.KAFKA_R_TOPIC_PARTITIONS_NUM)
                .assignTimestampsAndWatermarks(new MyCommonTestWatermarkPeriodicAssigner());
        DataStream<Tuple4<String, String, String, String>> secondStream = CommonKafkaInputStreamGenerate.getSecondInputStream(env)
                .map(new MapStringToTuple4()).setParallelism(GeneralParameters.KAFKA_S_TOPIC_PARTITIONS_NUM)
                .assignTimestampsAndWatermarks(new MyCommonTestWatermarkPeriodicAssigner());

        //键值选择器，两个流的键值都是第2个元素为键值
        KeySelector<Tuple4<String, String, String, String>, Double> keySelector = new KeySelector<Tuple4<String, String, String, String>, Double>() {
            @Override
            public Double getKey(Tuple4<String, String, String, String> value) throws Exception {
                return Double.parseDouble(value.f1);
            }
        };

        //构建合并的数据流
        CommonJoinUnionStream<Tuple4<String, String, String, String>, Tuple4<String, String, String, String>> myUnionStream = null;


        //根据标识的不同初始化不同模型的合并数据流
        switch (experimentModel) {

            case GeneralParameters.CO_MODEL:
                myUnionStream = new CoModelUnionStream<>(firstStream, secondStream, keySelector, keySelector);
                logger.info(">>>Experiment当前为CoModel模型");
                break;
        }


        //进行连接，产生结果流
        DataStream<CommonJoinUnionType<Tuple4<String, String, String, String>, Tuple4<String, String, String, String>>> resultStream =
                myUnionStream.bandNMJoinWithTimeWindow(Time.seconds(600), Time.seconds(600), r_surpass_s, r_behind_s);


        //==================只在当前程序中计算元组延迟，而将得到的结果输送到Kafka中的结果处理方式==================
        //        当前程序不负责计算平均延迟，需要之后另写程序从kafka中读取数据进行进一步处理，因而不会有窗口触发问题
        //设置Kafka Sink的相关参数
        //设置kafka的相关参数
        Properties kafkaSinkProperties = new Properties();
        kafkaSinkProperties.setProperty("bootstrap.servers", GeneralParameters.KAFKA_BROKER_LIST);
        //结果处理
        resultStream
                .map(new ComputeResultTupleLatencyMapFunction())         //计算每个结果元组的延迟
                .setParallelism(GeneralParameters.TOTAL_NUM_OF_TASK)    //需要设置并行度与Joiner数量一致
                .flatMap(new ParallelAccumulateResultLatencyMapFunction())    //在每个并行实例上分别累计每一秒的延迟
                .setParallelism(GeneralParameters.TOTAL_NUM_OF_TASK)    //需要设置并行度与Joiner数量一致
                .addSink(new FlinkKafkaProducer011<String>(GeneralParameters.TEST_RESULTS_COLLECT_KAFKA_TOPIC, new SimpleStringSchema(), kafkaSinkProperties, Optional.ofNullable(null)))
                .setParallelism(GeneralParameters.KAFKA_RESULTS_COLLECT_TOPIC_PARTITIONS_NUM);    //设置Sink的并行度与Kafka中分区数量一致

        env.execute();
    }

    /**
     * 将程序的输入参数上传到Zookeeper中以供其余组件读取
     */
    private void uploadExpConfigToZookeeper() throws InterruptedException {

        System.out.println("当前程序输入的配置参数为：" + args);

        // 初始化基于Zookeeper的协调器，用于上传程序参数
        ZookeeperBasedSynchronizer zookeeperBasedSynchronizer =
                new ZookeeperBasedSynchronizer(GeneralParameters.CONNECT_STRING, GeneralParameters.SESSION_TIMEOUT);
        // 创建永久节点
        zookeeperBasedSynchronizer.creatZookeeperNodePersistent(
                ParseExperimentParametersTool.ZOOKEEPER_EXP_CONFIGS_PATH_OF_ALL_MODEL,
                "config".getBytes());
        // 将当前程序的参数上传到Zookeeper
        zookeeperBasedSynchronizer.setZookeeperNodeContent(
                ParseExperimentParametersTool.ZOOKEEPER_EXP_CONFIGS_PATH_OF_ALL_MODEL,
                ParseExperimentParametersTool.translateStringArrayToByteArray(args));

        //程序休眠一段时间，以等待数据上传完成，之后关闭Zookeeper
        Thread.sleep(500);
        System.out.println("参数成功上传到Zookeeper！");
        System.out.println("当前程序从Zookeeper获取的配置为："
                + new String(zookeeperBasedSynchronizer
                .getZookeeperNodeContent(ParseExperimentParametersTool.ZOOKEEPER_EXP_CONFIGS_PATH_OF_ALL_MODEL)));
        zookeeperBasedSynchronizer.close();
    }

    /**
     * 将从kafka中读取的字符串数据流转化为Tuple类型
     * tuple-4中一般含义为：< 数据源编号【可以为Null，一般无用】-- 键值 -- 时间戳 -- 负载【即原本的整个字符串】>
     */
    private static class MapStringToTuple4 implements MapFunction<String, Tuple4<String, String, String, String>> {
        @Override
        public Tuple4<String, String, String, String> map(String value) throws Exception {
            String[] split = value.split(",");
            return new Tuple4<>(split[0],split[1],split[2],split[3]);
        }
    }
}
