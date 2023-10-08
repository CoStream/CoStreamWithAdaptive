package coModel.replaceable.joiners;

import base.CommonJoinUnionType;
import coModel.CoModelParameters;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import tools.index.JoinerLinkedIndexListOfCommonTypePool;

import java.util.List;

/**
 * 采用在内存中对对象级别的存储结构采用池化技术减少垃圾回收开销的Joiner
 */
public class CoModelJoinerWithCommonTypePoolMemoryIndex<F, S> extends AbstractCoModelCommonJoiner<F, S> {

    //本地用于存储元组的链式索引，其中包含有若干子索引
    private JoinerLinkedIndexListOfCommonTypePool<F, S, Double> localLinkedListIndex;

    /**
     * 有参构造器，进行范围连接的相关初始化参数设置
     */
    public CoModelJoinerWithCommonTypePoolMemoryIndex(Time r_TimeWindows, Time s_TimeWindows, KeySelector<F, Double> keySelector_R, KeySelector<S, Double> keySelector_S, double r_surpass_S, double r_behind_S) {
        super(r_TimeWindows, s_TimeWindows, keySelector_R, keySelector_S, r_surpass_S, r_behind_S);
    }

    /**
     * 初始化存储结构
     */
    @Override
    protected void initStoreDataStructureForBothStream() {

        logger.info("当前为采用在内存中对对象级别的存储结构采用池化技术减少垃圾回收开销的Joiner");

        long localSubIndexNum;
        if (isLocalJoinerTheRStoreNode(CoModelParameters.TOTAL_R_JOINER_NUM)) {
            localSubIndexNum = R_TimeWindows.toMilliseconds() / subTreeTimeWindowInterval;
        } else {
            localSubIndexNum = S_TimeWindows.toMilliseconds() / subTreeTimeWindowInterval;
        }
        //留下3个子树的冗余
        localSubIndexNum += 3;

        //初始化存储结构
        localLinkedListIndex = new JoinerLinkedIndexListOfCommonTypePool<>(
                (int) localSubIndexNum,
                (int) (CoModelParameters.MAX_STORE_TUPLE_CAPACITY_IN_EACH_JOINER / localSubIndexNum),
                subTreeTimeWindowInterval);

        logger.info("Joiner-" + subTaskIdx
                + ":内存池初始化成功！其中子索引数量为：" + (int) localSubIndexNum
                + "；每个子索引的容量为：" + (int) (CoModelParameters.MAX_STORE_TUPLE_CAPACITY_IN_EACH_JOINER / localSubIndexNum)
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
//        value.clear();  // 以防万一，这里也不清空元组，性能没什么影响，还有可能产生错误
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
        logger.info("Joiner-" + subTaskIdx + ":过期+前+各个子索引的内存占用详细情况（已存储元组数，最大容量，当前最大时间戳）为："
                + localLinkedListIndex.getStoreStatusOfAllSubIndex());
        localLinkedListIndex.expire(coMinTimestamp);
        logger.info("Joiner-" + subTaskIdx + ":过期-后-各个子索引的内存占用详细情况（已存储元组数，最大容量，当前最大时间戳）为："
                + localLinkedListIndex.getStoreStatusOfAllSubIndex());
    }
}
