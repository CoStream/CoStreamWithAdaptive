package coModel.replaceable.joiners;

import base.CommonJoinUnionType;
import coModel.CoModelParameters;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import tools.index.offHeap.CommonJoinUnionTypeSerializer;
import tools.index.offHeap.JoinerLinkedIndexListOfOffHeapPool;

import java.util.List;

/**
 * 采用在内存中基于字节缓冲池的存储结构采用池化技术减少垃圾回收开销的Joiner
 */
public class CoModelJoinerWithOffHeapMemoryPoolIndex<F, S> extends AbstractCoModelCommonJoiner<F, S> {

    //本地用于存储元组的链式索引，其中包含有若干子索引(字节缓冲池)
    private JoinerLinkedIndexListOfOffHeapPool<F, S, Double> localLinkedListIndex;
    //用于堆外内存的序列化器
    private CommonJoinUnionTypeSerializer<F, S> serializer;

    /**
     * 有参构造器，进行范围连接的相关初始化参数设置
     */
    public CoModelJoinerWithOffHeapMemoryPoolIndex(Time r_TimeWindows, Time s_TimeWindows, KeySelector<F, Double> keySelector_R, KeySelector<S, Double> keySelector_S, double r_surpass_S, double r_behind_S, CommonJoinUnionTypeSerializer<F, S> serializer) {
        super(r_TimeWindows, s_TimeWindows, keySelector_R, keySelector_S, r_surpass_S, r_behind_S);
        this.serializer = serializer;
    }

    /**
     * 初始化存储结构
     */
    @Override
    protected void initStoreDataStructureForBothStream() {

        logger.info("当前为采用堆外内存池的存储结构减少垃圾回收开销的Joiner");

        long localSubIndexNum;
        if (isLocalJoinerTheRStoreNode(CoModelParameters.TOTAL_R_JOINER_NUM)) {
            localSubIndexNum = R_TimeWindows.toMilliseconds() / subTreeTimeWindowInterval;
        } else {
            localSubIndexNum = S_TimeWindows.toMilliseconds() / subTreeTimeWindowInterval;
        }
        //留下3个子树的冗余
        localSubIndexNum += 3;

        //初始化存储结构
        localLinkedListIndex = new JoinerLinkedIndexListOfOffHeapPool<>(
                (int) localSubIndexNum,
                CoModelParameters.MAX_BYTE_CONSUME_IN_EACH_JOINER / localSubIndexNum,
                subTreeTimeWindowInterval,
                serializer);

        logger.info("Joiner-" + subTaskIdx
                + ":<字节数组>内存池初始化成功！其中子索引数量为：" + (int) localSubIndexNum
                + "；每个子索引的容量（字节数）为：" + CoModelParameters.MAX_BYTE_CONSUME_IN_EACH_JOINER / localSubIndexNum
                + "；子索引保存的元组时间范围为：" + subTreeTimeWindowInterval);
    }

    @Override
    protected void deleteAllOutOfRangeTuplesInSpecialSubTreeSet(Tuple2<Integer, Integer> localStoreSerialNumRange, boolean isRStore) {
        logger.info("暂时未实现删除不在范围内的元组的功能！");
    }

    /**
     * 将指定的元组插入到对应的存储结构中,当子窗口的时间跨度达到阈值时，新建子索引插入
     * @param value             输入元组
     * @param key               元组对应的键值
     * @param timestamp         元组对应的时间戳
     * @param isRStoreIndex     要插入的存储结构是R还是S。True：插入R中。
     */
    @Override
    protected void insertTupleToEachStoreStructure(CommonJoinUnionType<F, S> value, Double key, long timestamp, boolean isRStoreIndex) {
        value.setSelfTimestamp(timestamp);
        localLinkedListIndex.insertNewTuple(key, value);
    }

    /**
     * 查找指定存储结构中所有在给定的时间和键值范围内的元组
     * @param minTimestamp 要连接元组的最小时间戳，由输入元组的时间戳和时间窗口大小决定
     * @param minKey 要查找的最小键值(包含)
     * @param maxKey 要查找的最大键值（包含）
     * @param isProbe_R_StoreIndex 要查找的存储结构是R还是S。True：探测R的索引结构
     * @return 返回所有满足时间条件和键值范围的元组
     */
    @Override
    protected List<CommonJoinUnionType<F, S>> getAllMatchedTuplesForEachStream(long minTimestamp, Double minKey, Double maxKey, boolean isProbe_R_StoreIndex) {
        return localLinkedListIndex.findRangeCeilingMinTimestamp(minKey, true, maxKey, true, minTimestamp);
    }

    @Override
    protected Tuple2<Long, Long> getStoreStatusOfEachStoreStructure(boolean isRStore) {
        return localLinkedListIndex.getIndexLinkedListStoreStatus();
    }

    @Override
    protected void expireEachStoreStructure(boolean isRStoreIndexExpire, long coMinTimestamp) {
        logger.info("Joiner-" + subTaskIdx + ":过期前各个子索引的内存占用详细情况（已存储元组数，已存储字节数，最大字节数容量，当前最大时间戳）为（方法在窗口大小不同时有问题）："
                + localLinkedListIndex.getStoreStatusOfAllSubIndex());
        localLinkedListIndex.expire(coMinTimestamp);
    }

    @Override
    public void close() throws Exception {
        super.close();
        localLinkedListIndex.destroy();
    }
}
