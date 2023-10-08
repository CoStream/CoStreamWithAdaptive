package coModel.adaptive;

import base.CommonJoinUnionType;
import coModel.CoModelParameters;
import coModel.adaptive.index.CommonJoinUnionTypeSerializerForAdaptive;
import coModel.adaptive.index.JoinerLinkedIndexListOfOffHeapPoolForAdaptive;
import coModel.tools.sync.ZookeeperNodePathSetForCoModel;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.List;

public class CoModelJoinerWithAdaptive<F, S> extends AbstractCoModelCommonJoinerWithAdaptive<F, S> {

    //本地用于存储元组的链式索引，其中包含有若干子索引(字节缓冲池)
    private JoinerLinkedIndexListOfOffHeapPoolForAdaptive<F, S, Double> localLinkedListIndex;
    //用于堆外内存的序列化器
    private CommonJoinUnionTypeSerializerForAdaptive<F, S> serializer;

    /**
     * 有参构造器，进行范围连接的相关初始化参数设置
     */
    public CoModelJoinerWithAdaptive(Time r_TimeWindows, Time s_TimeWindows, KeySelector<F, Double> keySelector_R, KeySelector<S, Double> keySelector_S, double r_surpass_S, double r_behind_S,  CommonJoinUnionTypeSerializerForAdaptive<F, S> serializer) {
        super(r_TimeWindows, s_TimeWindows, keySelector_R, keySelector_S, r_surpass_S, r_behind_S);
        this.serializer = serializer;
    }

    /**
     * 初始化存储结构
     */
    @Override
    protected void initStoreDataStructureForBothStream() {
        logger.info("当前为采用堆外内存池的存储结构减少垃圾回收开销的Joiner，且具有自适应功能");
        // 每个窗口的子索引数量
        int localSubIndexNum = CoModelParameterWithAdaptive.NUM_OF_SUB_INDEX;
        // 子索引数量需要加上一些冗余
        localSubIndexNum += CoModelParameterWithAdaptive.REDUNDANCY_NUM_OF_SUB_INDEX;

        //获取当前Joiner在对应的存储部分的行号
        int rowNumLocally = getRowNumLocally(subTaskIdx, R_copiesNum, S_copiesNum);

        // 初始化本地链式子索引
        if (isLocalJoinerTheRStoreNode()) {
            localLinkedListIndex = new JoinerLinkedIndexListOfOffHeapPoolForAdaptive<>(localSubIndexNum,
                    (CoModelParameters.MAX_BYTE_CONSUME_IN_EACH_JOINER / localSubIndexNum),
                    subTreeTimeWindowIntervalOf_R,
                    serializer,
                    R_copiesNum,
                    rowNumLocally);
            logger.info("Joiner-" + subTaskIdx
                    + ":<字节数组>R内存池初始化成功！其中子索引数量为：" + localSubIndexNum
                    + "；每个子索引的容量（字节数）为：" + CoModelParameters.MAX_BYTE_CONSUME_IN_EACH_JOINER / localSubIndexNum
                    + "；R子索引保存的元组时间范围为(ms)：" + subTreeTimeWindowIntervalOf_R
                    + "；存储R的元组，且R复制数为：" + R_copiesNum
                    + "；当前Joiner对应的行号为：" + rowNumLocally);
        } else {
            localLinkedListIndex = new JoinerLinkedIndexListOfOffHeapPoolForAdaptive<>(localSubIndexNum,
                    (CoModelParameters.MAX_BYTE_CONSUME_IN_EACH_JOINER / localSubIndexNum),
                    subTreeTimeWindowIntervalOf_S,
                    serializer,
                    S_copiesNum,
                    rowNumLocally);
            logger.info("Joiner-" + subTaskIdx
                    + ":<字节数组>S内存池初始化成功！其中子索引数量为：" + localSubIndexNum
                    + "；每个子索引的容量（字节数）为：" + CoModelParameters.MAX_BYTE_CONSUME_IN_EACH_JOINER / localSubIndexNum
                    + "；S子索引保存的元组时间范围为(ms)：" + subTreeTimeWindowIntervalOf_S
                    + "；存储S的元组，且S复制数为：" + S_copiesNum
                    + "；当前Joiner对应的行号为：" + rowNumLocally);
        }
    }

    @Override
    protected void monitorMemoryFootprintAndUpload() {
        //当前活动子索引以及上一个归档的子索引的内存占用
        long activeSubIndexMem = localLinkedListIndex.getMemoryFootprintOffsetCurrent(0);
        long latestArchiveSubIndexMem = localLinkedListIndex.getMemoryFootprintOffsetCurrent(1);
        //获取总内存占用
        long totalMemoryFootprint = localLinkedListIndex.getTotalMemoryFootprint();
        //上传两个子索引的内存占用，格式为：当前活动子索引内存占用，上一个归档的子索引的内存占用，总内存占用
        synchronizer.setZookeeperNodeContent(
                ZookeeperNodePathSetForCoModel.JOINER_UPLOAD_SUB_INDEX_MEM_COMMON_PREFIX + subTaskIdx,
                ("" + activeSubIndexMem + "," + latestArchiveSubIndexMem + "," + totalMemoryFootprint).getBytes());

    }

    @Override
    protected void executeOperationAfterAlignment(int subTaskIdx, int storeNodeNumOf_R, int copiesNumOf_R, int storeNodeNumOf_S, int copiesNumOf_S) {

        int rowNumLocally = getRowNumLocally(subTaskIdx, copiesNumOf_R, copiesNumOf_S);  // 获取行号
        if (isLocalJoinerTheRStoreNode()) {  // 新建子索引，并更新子索引的复制数和行号
            localLinkedListIndex.resetCopyAndRowNumAndCreateNewSubIndex(copiesNumOf_R, rowNumLocally);
        } else {
            localLinkedListIndex.resetCopyAndRowNumAndCreateNewSubIndex(copiesNumOf_S, rowNumLocally);
        }

        // 更新本地的复制数
        R_copiesNum = copiesNumOf_R;
        S_copiesNum = copiesNumOf_S;

        logger.info("Joiner-" + subTaskIdx
                + "完成对齐后的内存索引调整操作，调整后所有子索引的R复制数，S复制数，以及对应行号分别为： "
                + R_copiesNum + ";" + S_copiesNum + ";" + rowNumLocally);
    }

    @Override
    protected void insertTupleToEachStoreStructure(CommonJoinUnionType<F, S> value, Double key, long timestamp, boolean isRStoreIndex) {
        value.setSelfTimestamp(timestamp);
        localLinkedListIndex.insertNewTuple(key, value);
    }

    @Override
    protected List<CommonJoinUnionType<F, S>> getAllMatchedTuplesForEachStream(long minTimestamp, Double minKey, Double maxKey, boolean isProbe_R_StoreIndex, long serialNum) {
        return localLinkedListIndex.findRangeCeilingMinTimestampWitSerial(minKey, true, maxKey, true, minTimestamp, serialNum);
    }

    @Override
    protected Tuple2<Long, Long> getStoreStatusOfEachStoreStructure(boolean isRStore) {
        return localLinkedListIndex.getIndexLinkedListStoreStatus();
    }

    @Override
    protected void expireEachStoreStructure(boolean isRStoreIndexExpire, long coMinTimestamp) {

        if (isRStoreIndexExpire == isLocalJoinerTheRStoreNode()) {
            if (isLocalJoinerTheRStoreNode()) {
                logger.info("Joiner-" + subTaskIdx + ":为R的存储节点，过期前各个子索引的内存占用详细情况（已存储元组数，已存储字节数，最大字节数容量，当前最大时间戳，对应复制数，对应行号）为："
                        + localLinkedListIndex.getStoreStatusOfAllSubIndex());
                localLinkedListIndex.expire(coMinTimestamp);
            } else {
                logger.info("Joiner-" + subTaskIdx + ":为S的存储节点，过期前各个子索引的内存占用详细情况（已存储元组数，已存储字节数，最大字节数容量，当前最大时间戳，对应复制数，对应行号）为："
                        + localLinkedListIndex.getStoreStatusOfAllSubIndex());
                localLinkedListIndex.expire(coMinTimestamp);
            }
        }

    }

    @Override
    public void close() throws Exception {
        super.close();
        localLinkedListIndex.destroy();
    }
}
