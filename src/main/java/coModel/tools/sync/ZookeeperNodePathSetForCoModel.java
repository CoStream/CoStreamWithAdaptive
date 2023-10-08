package coModel.tools.sync;

import coModel.CoModelParameters;

import java.io.Serializable;

/**
 * 所有用于系统组件间通讯的zookeeper节点路径
 * 其中路径的前缀指的是后接多个实例编号的路径，前缀和编号之间必须用’-‘分割，以便于后续提取实例号
 */
public class ZookeeperNodePathSetForCoModel implements Serializable {
    //用于序列化，后加的，序列化也是后加的
    private static final long serialVersionUID = 1044404004L;

    //用于标志系统是否存活的节点，若该节点消失，则标识系统已经死亡，所有系统相关的组件均应该死亡
    //--在此，该节点由启动协调器的Router作为临时节点创建，主要用于通知协调器系统死亡
    //--该节点应该在系统运行的最开始由创建协调器的线程只创建一遍，后续不可更改
    public static final String CO_MODEL_SYSTEM_ALIVE = CoModelParameters.ZOOKEEPER_PREFIX_OF_CO_MODEL + "/isAlive";

    //用于Joiner通知协调器通用相关消息的节点
    public static final String JOINER_NOTIFY_COORDINATOR_COMMON_PREFIX = CoModelParameters.ZOOKEEPER_PREFIX_OF_CO_MODEL_JOINER + "/notifyCoordinator" + "-";
    //用于Joiner接收协调器通用相关消息的节点
    public static final String JOINER_RECEIVE_COORDINATOR_COMMON_PREFIX = CoModelParameters.ZOOKEEPER_PREFIX_OF_CO_MODEL_JOINER + "/receiveCoordinator" + "-";
    //用于Joiner上传当前活动子索引以及上一个归档的子索引的内存占用的节点前缀，格式为：当前活动子索引内存占用，上一个归档的子索引的内存占用，总内存占用
    public static final String JOINER_UPLOAD_SUB_INDEX_MEM_COMMON_PREFIX = CoModelParameters.ZOOKEEPER_PREFIX_OF_CO_MODEL_JOINER + "/subIndexMem" + "-";

    //用于Router通知协调器通用相关消息的节点
    public static final String ROUTER_NOTIFY_COORDINATOR_COMMON_PREFIX = CoModelParameters.ZOOKEEPER_PREFIX_OF_CO_MODEL_ROUTER + "/notifyCoordinator" + "-";
    //用于Router接收协调器通用相关消息的节点(由于所有Router相同，所以共用一个接收节点，此处不为前缀)
    public static final String ROUTER_RECEIVE_COORDINATOR_COMMON_NODE = CoModelParameters.ZOOKEEPER_PREFIX_OF_CO_MODEL_ROUTER + "/receiveCoordinator";
    //用于Router表示自身监控的两个流的输入速率的节点前缀（后接具体编号）(内容形式为字符串：-R速率,S速率-)
    public static final String ROUTER_MONITOR_TWO_STREAM_INPUT_RATE_PREFIX = CoModelParameters.ZOOKEEPER_PREFIX_OF_CO_MODEL_ROUTER + "/inputRate" + "-";


    //用于与监控器有关的节点信息
    //监控器通知各个Joiner上传各自的负载信息的节点，所有Joiner共用一个通知节点
    public static final String MONITOR_NOTIFY_JOINER_UPLOAD_WORKLOAD = CoModelParameters.ZOOKEEPER_PREFIX_OF_CO_MODEL_MONITOR + "/mToJForUpload";
    //每个Joiner上传各自的负载信息节点的前缀
    public static final String JOINER_UPLOAD_ITSELF_WORKLOAD_TO_MONITOR_PREFIX = CoModelParameters.ZOOKEEPER_PREFIX_OF_CO_MODEL_MONITOR + "/joinerWorkload" + "-";

    //每个Joiner上传各自的存储元组执行时间和连接元组执行时间的前缀
    public static final String JOINER_UPLOAD_LOCAL_STORE_TUPLE_PROCESS_TIME_PREFIX = CoModelParameters.ZOOKEEPER_PREFIX_OF_CO_MODEL_MONITOR + "/joinerStoreTime" + "-";
    public static final String JOINER_UPLOAD_LOCAL_JOIN_TUPLE_PROCESS_TIME_PREFIX = CoModelParameters.ZOOKEEPER_PREFIX_OF_CO_MODEL_MONITOR + "/joinerJoinTime" + "-";



    /*-Joiner内存变更同步回合-
    一对多回合，用于协调器通知Router发送同步信号（信号中包含最新的分区方案），之后Joiner接收Router的同对齐消息，之后Joiner执行内存改变操作，
    当所有Joiner的内存改变操作完成后，回合结束*/
    public static final SyncRound C_R_J_CHANGE_JOINER_MEMORY_SYNC_ROUND
            = new SyncRound(CoModelParameters.ZOOKEEPER_PREFIX_OF_CO_MODEL_ROUTER + "/changeMemoryAlignmentN",
            CoModelParameters.ZOOKEEPER_PREFIX_OF_CO_MODEL_JOINER + "/changeMemoryAlignmentR" + "-");


}
