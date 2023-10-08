package coModel;

import common.GeneralParameters;

/**
 * CoModel中要用到的参数
 */
public class CoModelParameters {

    //总共的Router的数量,参照通用设置
    public static final int TOTAL_NUM_OF_ROUTER = GeneralParameters.NUM_OF_ROUTER;//GeneralParameters.NUM_OF_ROUTER;
    //总共的joiner数量,参照通用设置
    public static final int TOTAL_NUM_OF_JOINER = GeneralParameters.TOTAL_NUM_OF_TASK;//GeneralParameters.TOTAL_NUM_OF_TASK;

    //负责存储R流中元组以及S流中元组的Joiner节点数量（默认各为总数量的一半）
    public static final int TOTAL_R_JOINER_NUM = TOTAL_NUM_OF_JOINER / 2;
    public static final int TOTAL_S_JOINER_NUM = TOTAL_NUM_OF_JOINER - TOTAL_R_JOINER_NUM;

    //初始的R流中元组和S流中元组的存储副本数量（在程序运行过程中可能发生改变,此处在一开始设置为最大值）
    //若想让程序退化为二部图，则该值设为1
    public static final int INIT_R_NMU_OF_COPIES = 1;//TOTAL_R_JOINER_NUM;
    public static final int INIT_S_NMU_OF_COPIES = 1;//TOTAL_S_JOINER_NUM;

    //采用固定时间间隔子索引时，每个子索引的时间跨度(单位：ms，初始为30秒)
    public static final long SUB_INDEX_TIME_INTERVAL = GeneralParameters.SUB_INDEX_TIME_INTERVAL;

    //用于判断系统的内存是否已经过载，若系统剩余内存小于该值，则判定系统内存过载(单位：MB)
    //若想要不启动内存监控，则该值设为-1
    public static final int SYSTEM_MAX_REMAIN_MEMORY = -1;//1024;

    //在利用zookeeper进行通讯时需要用到的节点路径前缀
    public static final String ZOOKEEPER_PREFIX_OF_CO_MODEL = "/CoModel";
    public static final String ZOOKEEPER_PREFIX_OF_CO_MODEL_COORDINATOR = "/CoModel/Coordinator";
    public static final String ZOOKEEPER_PREFIX_OF_CO_MODEL_ROUTER = "/CoModel/Router";
    public static final String ZOOKEEPER_PREFIX_OF_CO_MODEL_JOINER = "/CoModel/Joiner";
    public static final String ZOOKEEPER_PREFIX_OF_CO_MODEL_MONITOR = "/CoModel/Monitor";

    //设置每个Joiner理论上的存储元组数量上限（所有子索引存储元组数量的和）
    //这是为了采用元组级别的内存池化技术时设置内存池大小的上限
    public static final int MAX_STORE_TUPLE_CAPACITY_IN_EACH_JOINER = 6000 * 600;
    //设置每个Joiner可以申请的用于存储元组的字节数组的最大值
    //这是为例采用字节数组内存池时设置的单个slot的占用字节数组长度上限（单位：字节）(也是单个Slot堆外内存内存池的存储上限)
    public static final int MAX_BYTE_CONSUME_IN_EACH_JOINER = 512 * 1024 * 1024;



}
