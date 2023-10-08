package experiment;

import base.CommonJoinUnionStream;
import base.CommonJoinUnionType;
import coModel.CoModelParameters;
import coModel.CoModelUnionStream;
import common.GeneralParameters;
import experiment.component.MyCommonTestWatermarkPeriodicAssigner;
import input.CommonKafkaInputStreamGenerate;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.log4j.Logger;

import java.io.Serializable;

//import sepModel.SepModelUnionStream;
//import splitModel.SplitModelUnionStream;

/**
 * 将执行实验的流程封装成一个类，此类实验验证不同方法的正确性
 */
public class ExperimentExecutorForCorrect implements Serializable {
    //用于标识是在进行哪个实验
    private String experimentModel;
    //在进行测试时的并行度
    private int parallelism = CoModelParameters.TOTAL_NUM_OF_ROUTER;

    //获取logger对象用于日志记录
    private static final Logger logger = Logger.getLogger(ExperimentExecutorForCorrect.class.getName());

    public ExperimentExecutorForCorrect(String experimentModel, int parallelism) {
        this.experimentModel = experimentModel;
        this.parallelism = parallelism;
    }


    /**
     * 实际进行实验
     * @throws Exception
     */
    public void startExperiment() throws Exception {

        logger.info(">>>开始执行ExperimentExecutorForCorrect实验,测试并行度为：" + parallelism);

        //获取环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置事件时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //设置发送水位线的时间间隔,500ms
        env.getConfig().setAutoWatermarkInterval(500);
        //设置网络传输刷新时间
        env.setBufferTimeout(5);
        //设置并行度
        env.setParallelism(parallelism);

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
            case GeneralParameters.SEP_MODEL:
                //myUnionStream = new SepModelUnionStream<>(firstStream, secondStream, keySelector, keySelector);
                logger.info(">>>Experiment当前为我们的分离模型");
                break;
            //split模型
            case GeneralParameters.SPLIT_MODEL:
                //myUnionStream = new SplitModelUnionStream<>(firstStream, secondStream, keySelector, keySelector);
                logger.info(">>>Experiment当前为split模型 ");
                break;
            //混合模型
            case GeneralParameters.CO_MODEL:
                myUnionStream = new CoModelUnionStream<>(firstStream, secondStream, keySelector, keySelector);
                logger.info(">>>Experiment当前为CoModel模型 ");
                break;

        }


        //进行连接，产生结果流
        DataStream<CommonJoinUnionType<Tuple4<String, String, String, String>, Tuple4<String, String, String, String>>> resultStream =
                myUnionStream.bandNMJoinWithTimeWindow(Time.seconds(600), Time.seconds(600), 0, 0);


        //结果处理
        resultStream.print();

        env.execute();
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
