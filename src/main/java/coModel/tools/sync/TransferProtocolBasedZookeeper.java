package coModel.tools.sync;

import org.apache.flink.api.java.tuple.Tuple4;

import java.io.Serializable;

/**
 * 定义Router、Joiner以及Coordinator之间通过Zookeeper传输的通知消息类型
 */
public class TransferProtocolBasedZookeeper implements Serializable {

    //用于序列化，后加的，序列化也是后加的
    private static final long serialVersionUID = 1044404005L;

    //Joiner上传的，指示Joiner内存过载的消息类型
    public static final long JOINER_OVER_MEMORY = 0x1L;

    //中央协调器用于通知Router分区方案发生改变的消息类型
    public static final long CHANGE_COPIES_NUM = 0x2L;

    //中央协调器用于通知Router内存变更回合结束，可以开始正常处理的消息
    public static final long JOINER_SYNC_COMPLETE = 0x4L;


    private long eventType = 0L;

    private String content;

    /**
     * 无参构造器
     */
    public TransferProtocolBasedZookeeper() {

    }

    /**
     * 接收端构建一个要解析的内容
     */
    public TransferProtocolBasedZookeeper(byte[] transferContent) {
        String[] split = new String(transferContent).split("-");
        eventType = Long.parseLong(split[0]);
        content = split[1];

    }

    /**
     * 发送端构建一个上传的节点内容
     */
    public TransferProtocolBasedZookeeper(long eventType, String content) {
        this.eventType = eventType;
        this.content = content;
    }


    /**
     * 创建一个指示Joiner内存过载的消息
     * @param content 要放在消息中的实际内容的字符串形式
     */
    public static TransferProtocolBasedZookeeper createJoinerOverMemoryMess(String content) {
        return new TransferProtocolBasedZookeeper(JOINER_OVER_MEMORY, content);
    }

    /**
     * 根据给定的分区方案，创建并返回一个用于通知Router和Joiner每个流的副本数量发生改变的消息类型
     * @param partitionScheme 分区方案，其格式为：-R流存储节点数量，R流元组副本数量，S流存储节点数量，S流元组副本数量-
     */
    public static TransferProtocolBasedZookeeper createChangeCopiesNumMess(Tuple4<Integer, Integer, Integer, Integer> partitionScheme) {
        return new TransferProtocolBasedZookeeper(CHANGE_COPIES_NUM,
                "" + partitionScheme.f0 + "," + partitionScheme.f1 + "," + partitionScheme.f2 + "," + partitionScheme.f3);
    }


    /**
     * 创建一个用于通知Router，所有的Joiner同步完成的消息，表示Router可以进行正常的数据处理
     */
    public static TransferProtocolBasedZookeeper createNotifyJoinerSyncCompleteMess() {
        return new TransferProtocolBasedZookeeper(JOINER_SYNC_COMPLETE, "JoinerSyncComplete");
    }


    /**
     * 获取要通过Zookeeper传输的二进制内容
     * @return 要上传的zookeeper节点的内容
     */
    public byte[] getTransferByteForThisMess() {
        return ("" + eventType + "-" + content).getBytes();
    }

    /**
     * 判断是否为Joiner内存过载的消息
     */
    public boolean isJoinerOverMemoryMess() {
        return (eventType & JOINER_OVER_MEMORY) != 0;
    }

    /**
     * 判断是否为改变Joiner中副本数量的消息
     * 该消息用于中央协调器通知Router和Joiner
     */
    public boolean isChangeCopiesNum() {
        return (eventType & CHANGE_COPIES_NUM) != 0;
    }

    /**
     * 判断是否为通知Router，所有Joiner同步完成，可以开始正常的元组处理的消息
     */
    public boolean isNotifyJoinerSyncCompleteMess() {
        return (eventType & JOINER_SYNC_COMPLETE) != 0;
    }

    /**
     * 得到消息中包含的CoModel模型的当前分区方案
     * @return 分区方案，其格式为：-R流存储节点数量，R流元组副本数量，S流存储节点数量，S流元组副本数量-
     *          若当前消息不是改变分区方案的消息，则返回null
     */
    public Tuple4<Integer, Integer, Integer, Integer> getPartitionSchemeOfCoModel() {
        if (!isChangeCopiesNum()) {
            return null;
        }

        String[] split = content.split(",");
        return new Tuple4<Integer, Integer, Integer, Integer>(Integer.parseInt(split[0]),
                Integer.parseInt(split[1]),
                Integer.parseInt(split[2]),
                Integer.parseInt(split[3]));
    }


    /**
     * 获取消息的具体内容
     */
    public String getContent() {
        return content;
    }

    @Override
    public String toString() {
        return "{" + eventType + "-" + content + "}";
    }
}
