package common;

/**
 * 所有实验的一些通用参数
 */
public class GeneralParameters {
    //zookeeper集群地址
    public static final String CONNECT_STRING = "instance1:2181,instance2:2181,instance3:2181";
    //会话过期时间，即与集群断开超过该时间则判定当前会话结束
    public static final int SESSION_TIMEOUT = 400000;

    //kafka集群列表
    public static final String KAFKA_BROKER_LIST = "instance1:9092,instance2:9092,instance3:9092";
    //用于测试的两个kafka实时主题
    public static final String FIRST_KAFKA_REAL_TIME_TEST_TOPIC = "FirstInputTestTopicV1";
    public static final String SECOND_KAFKA_REAL_TIME_TEST_TOPIC = "SecondInputTestTopicV1";
    //用于收集结果的kafka主题
    public static final String TEST_RESULTS_COLLECT_KAFKA_TOPIC = "TestLatencyResultTopicV1";
    //Kafka结果收集主题的分区数量，该值用于确定Flink中sink算子的并行度（设置太多磁盘容量不够）
    public static final int KAFKA_RESULTS_COLLECT_TOPIC_PARTITIONS_NUM = 3;

    //总共的Router的数量
    public static final int NUM_OF_ROUTER = 64;
    //总共的joiner数量
    public static final int TOTAL_NUM_OF_TASK = 64;
    //如果采用二部图模型，则分别负责R与S流的joiner数量
    public static final int EACH_RELATION_NUM_OF_TASK = NUM_OF_ROUTER/2;
    //如果采用Sep模型，则S流的存储分区需要加上的偏移量占全部Joiner数量的比例
    public static final double SEP_S_STORE_PARTITION_OFFSET_RATIO = 0.5;

    //采用固定元组数量子索引Joiner时，每个子索引中元组的数量
    public static final long CONSTRAINT_SUB_INDEX_NUMBER = 10000;

    //实验所用的窗口时间(R和S的窗口大小都是该值，单位：秒)
    public static final long WINDOW_SIZE = 10 * 60;

    //实际用于存储元组的子树数量
    public static final int NUM_OF_SUB_INDEX = 5;

    //采用固定时间间隔子索引时，每个子索引的时间跨度(单位：ms，因此需要乘以1000以转化为ms)
    public static final long SUB_INDEX_TIME_INTERVAL = (WINDOW_SIZE / NUM_OF_SUB_INDEX) * 1000;

    //用于判断系统的内存是否已经过载，若系统剩余内存小于该值，则判定系统内存过载(单位：MB)
    public static final int SYSTEM_MAX_REMAIN_MEMORY = 50;


    //用于指示范围连接的参数
    //r_surpass_S 用于范围连接（以一个S元组的时间为参照物），即S - R_behind_S < R < S + R_surpass_S ;R - R_surpass_S < S < R + R_behind_S
    //r_behind_S 用于范围连接（以一个S元组的时间为参照物），即S - R_behind_S < R < S + R_surpass_S ;R - R_surpass_S < S < R + R_behind_S
    //若要执行等值连接，则该值均为 0
//    public static final double R_surpass_S = 0;
//    public static final double R_behind_S = 0;






    //kafka数据源中R流与S流数据源的并行度
    public static final int KAFKA_R_TOPIC_PARTITIONS_NUM = 3;
    public static final int KAFKA_S_TOPIC_PARTITIONS_NUM = 3;

    //用于定义水位线晚于最大时间戳的时间长度（单位：ms）
    public static final long WATERMARK_AFTER_MAX_TIMESTAMP = 500L;
    //Joiner中两棵子树的过期周期(ms)
    public static final long JOINER_EXPIRATION_PERIOD = SUB_INDEX_TIME_INTERVAL;

    //用于标识不同方法的字符串
    public static final String BI_MODEL = "biModel";    //二部图模型
    public static final String SEP_MODEL = "sepModel";    //SepJoin模型
    public static final String SPLIT_MODEL = "splitModel";    //Split模型
    public static final String MATRIX_MODEL = "matrixModel";    //矩阵模型
    public static final String CO_MODEL = "AdaptiveCoModel";    //本代码要验证的模型，即自适应混和模型
    public static final String CO_MODEL_FOR_FIX_OVERHEAD_TEST = "AdaptiveCoModelForFixOverloadTest";    //自适应混和模型的实验性质的测试，此时模型中每个存储元组和连接元组的开销是固定且指定的
    public static final String CO_MODEL_FOR_TIME_RECORD_TEST = "AdaptiveCoModelForTimeRecordTest";    //自适应混和模型的实验性质的测试，其中的每个Joiner会自发的周期性的上传其内部存储元组和连接元组的执行时间
    public static final String CO_MODEL_FOR_TREE_MAP_SUB_INDEX = "AdaptiveCoModelForTreeMap_1";    //自适应混和模型的实验性质的测试，Joiner采用的是TreeMap子索引
    public static final String CO_MODEL_FOR_SUB_B_PLUS_TREE_INDEX = "AdaptiveCoModelForSubBPlusTree_1";    //自适应混和模型的实验性质的测试，Joiner采用的是B+树子索引，其实与一般的CoModel相比就是加了个计时功能，此处单独拿出是用于区分
    public static final String CO_MODEL_FOR_FIX_JOIN_TIME_NO_STORE_TEST = "AdaptiveCoModelTestForFixJoinNoStore";  //自适应混和模型的实验性质的测试，连接元组处理时间固定为1ms，同时Router不转发存储元组
    public static final String CO_MODEL_FOR_COMMON_TYPE_POOL_TEST = "CO_MODEL_FOR_COMMON_TYPE_POOL_TEST";  //自适应混和模型的实验性质的测试，采用的是元组级别的内存池
    public static final String CO_MODEL_FOR_BYTE_POOL_TEST = "CO_MODEL_FOR_BYTE_POOL_TEST";  //自适应混和模型的实验性质的测试，采用的是基于字节数组的内存池
    public static final String MATRIX_FOR_COMMON_TYPE_POOL_TEST = "MATRIX_FOR_COMMON_TYPE_POOL_TEST";  //矩阵模型实验，采用的是元组级别的内存池
    public static final String CO_MODEL_OFF_HEAP_POOL_TEST = "CO_MODEL_OFF_HEAP_POOL_TEST";  //CoModel模型实验，采用的是堆外内存的内存池
    public static final String MATRIX_OFF_HEAP_POOL_TEST = "MATRIX_OFF_HEAP_POOL_TEST";  //矩阵模型实验，采用的是是堆外内存的内存池

    //要进行测试的TPC-H测试集中的最小ship date，用于日期到天的转换，用于提取日期键值
    public static final String MIN_SHIP_DATE ="1991-01-01";// "1992-01-02";

    //设置flink上下游算子之间的数据刷新周期，周期越小，整体延迟越低，若为0则表示每条记录会被立即发往下游
    //以下是flink源码解释的不同参数表示的意义
    //	 *   <li>A positive integer triggers flushing periodically by that integer</li>
    //	 *   <li>0 triggers flushing after every record thus minimizing latency</li>
    //	 *   <li>-1 triggers flushing only when the output buffer is full thus maximizing
    //	 *      throughput</li>
    public static final long FLINK_OUTPUT_FLUSH_PERIOD = 0L;

}
