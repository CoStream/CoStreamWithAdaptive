package coModel.tools;

import base.CommonJoinUnionType;

import java.io.Serializable;

/**
 * 在CoModel模型中使用，用于创造各种在系统上下游间传递的信号元组以及辅助信号元组类型的判断
 *  信号元组的创建基于系统传输的元组类型
 */
public class SignalMessageFactoryForCoModel<F, S> implements Serializable {

    //上游Router发送给Joiner的，用于对齐所有Joiner的消息类型
    //--用于在对齐Joiner后，在Joiner中执行一些操作，如增加内存或减少内存
    private static final long ROUTER_SEND_JOINER_ALIGNMENT_TYPE = 1L;

    /**
     * 判断一个输入元组是否是信号消息类型
     * @param inputTuple 输入元组
     * @return 若是信号消息，则返回true
     */
    public boolean isSignalMess(CommonJoinUnionType<F, S> inputTuple) {
        return !inputTuple.isJoinMode() && !inputTuple.isStoreMode();
    }

    /**
     * 创建Router发送给Joiner的对齐消息，Joiner通过该消息对齐，对齐后执行一些同步方法
     * @param routerIndex 发送方Router的任务编号
     * @param numPartition 要发往的Joiner分区编号
     * @return 要向下游发送的消息
     */
    public CommonJoinUnionType<F, S> creatNotifyJoinerAlignmentMess(int routerIndex, int numPartition) {
        CommonJoinUnionType<F, S> signalMess = new CommonJoinUnionType<>();
        signalMess.setJoinMode(false);
        signalMess.setStoreMode(false);
        signalMess.setNumPartition(numPartition);
        //用自己的时间戳字段标记发送方编号
        signalMess.setSelfTimestamp(routerIndex);
        //用Other时间戳字段标记消息类型
        signalMess.setOtherTimestamp(ROUTER_SEND_JOINER_ALIGNMENT_TYPE);
        return signalMess;
    }

    /**
     * 返回指定元组的消息类型
     * @param inputTuple 输入元组
     * @return 输入元组的消息类型
     */
    public long getSignalMessType(CommonJoinUnionType<F, S> inputTuple) {
        //如果该元组不是消息类型，则返回-1
        if (!isSignalMess(inputTuple)) {
            return -1;
        }
        //用Other时间戳字段标记消息类型
        return inputTuple.getOtherTimestamp();
    }

    /**
     * 如果该消息是指明Router停止发送消息的同步消息类型，则返回发送方的Router编号
     * @param inputTuple 输入元组
     * @return 该输入元组对应的发送方Router编号
     */
    public int getRouterIndexOfNotifyJoinerAlignmentMess(CommonJoinUnionType<F, S> inputTuple) {
        //如果该消息不是通知Joiner上游停止发送消息类型，则返回-1
        if (!isSignalMess(inputTuple) || (inputTuple.getOtherTimestamp() != ROUTER_SEND_JOINER_ALIGNMENT_TYPE)) {
            return -1;
        }
        //用自己的时间戳字段标记发送方编号
        return (int) inputTuple.getSelfTimestamp();
    }

    /**
     * 判断一个消息是否是由Router发送的对齐消息
     * @return 如果是对齐消息，返回true
     */
    public boolean isAlignmentMess(CommonJoinUnionType<F, S> inputTuple) {
        return getSignalMessType(inputTuple) == ROUTER_SEND_JOINER_ALIGNMENT_TYPE;
    }

}
