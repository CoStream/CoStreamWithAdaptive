package coModel.adaptive.index;

import base.CommonJoinUnionType;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple6;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * 由所有基于堆外内存的内存池的内存池子索引构成的子索引列表,包含自适应功能
 * @param <F> 第一个流的类型
 * @param <S> 第二个流的类型
 * @param <K> 存储的键值类型
 */
public class JoinerLinkedIndexListOfOffHeapPoolForAdaptive<F, S, K extends Comparable<K>> implements Serializable {
    //最大子索引数量
    private int maxSubIndexNum;
    //每个子索引的最大容量(字节数)
    private long maxByteCapacityOfSubIndex;
    //每个子索引存储元组的时间范围(ms)
    private long timeIntervalOfSubIndex;
    //子索引链表
    private List<JoinerSubIndexWithOffHeapPoolForAdaptive<F, S, K>> subIndexLinkedList;
    //当前活动子索引位置
    private int activeSubIndexPosition;
    //用于堆外内存的序列化器
    CommonJoinUnionTypeSerializerForAdaptive<F, S> serializer;

    //当前子索引对应的系统的复制数（用于判断哪些连接元组需要与本子索引进行连接）
    private int copyNum;
    //当前子索引对应的节点在当前复制数下的行号（用于判断哪些连接元组需要与本子索引进行连接）
    private int rowNum;

    /**
     * 不指定当前系统复制数与Joiner行号的构造器，此时与所有连接元组连接
     * @param maxSubIndexNum 最大子索引数量
     * @param maxByteCapacityOfSubIndex 每个子索引的最大容量(字节数)
     * @param timeIntervalOfSubIndex 每个子索引存储元组的时间范围（固定值）
     * @param serializer 子索引中要用到的序列化器
     */
    public JoinerLinkedIndexListOfOffHeapPoolForAdaptive(int maxSubIndexNum, long maxByteCapacityOfSubIndex, long timeIntervalOfSubIndex, CommonJoinUnionTypeSerializerForAdaptive<F, S> serializer) {
        this.maxSubIndexNum = maxSubIndexNum;
        this.maxByteCapacityOfSubIndex = maxByteCapacityOfSubIndex;
        this.timeIntervalOfSubIndex = timeIntervalOfSubIndex;
        this.serializer = serializer;

        //这里表示所有连接元组均进行连接
        this.copyNum = 1;
        this.rowNum = 0;

        // 初始化所有子索引
        subIndexLinkedList = new ArrayList<>(maxSubIndexNum);
        for (int i = 0; i < maxSubIndexNum; i++) {
            subIndexLinkedList.add(new JoinerSubIndexWithOffHeapPoolForAdaptive<>(maxByteCapacityOfSubIndex, serializer, copyNum, rowNum));
        }
        //设置当前活动子索引
        activeSubIndexPosition = 0;
        System.out.println("当前使用的是基于堆外内存的内存池-JoinerLinkedIndexListOfOffHeapPool,且该子索引与自适应能力对应！");
    }

    /**
     * 不指定当前系统复制数与Joiner行号的构造器，此时与所有连接元组连接
     *
     * @param maxSubIndexNum            最大子索引数量
     * @param maxByteCapacityOfSubIndex 每个子索引的最大容量(字节数)
     * @param timeIntervalOfSubIndex    每个子索引存储元组的时间范围（固定值）
     * @param serializer                子索引中要用到的序列化器
     * @param copyNum                   当前系统的复制数
     * @param rowNum                    当前Joiner对应的行号
     */
    public JoinerLinkedIndexListOfOffHeapPoolForAdaptive(int maxSubIndexNum,
                                                         long maxByteCapacityOfSubIndex,
                                                         long timeIntervalOfSubIndex,
                                                         CommonJoinUnionTypeSerializerForAdaptive<F, S> serializer,
                                                         int copyNum,
                                                         int rowNum) {

        this.maxSubIndexNum = maxSubIndexNum;
        this.maxByteCapacityOfSubIndex = maxByteCapacityOfSubIndex;
        this.timeIntervalOfSubIndex = timeIntervalOfSubIndex;
        this.serializer = serializer;

        //初始化复制数与行号
        this.copyNum = copyNum;
        this.rowNum = rowNum;

        // 初始化所有子索引
        subIndexLinkedList = new ArrayList<>(maxSubIndexNum);
        for (int i = 0; i < maxSubIndexNum; i++) {
            subIndexLinkedList.add(new JoinerSubIndexWithOffHeapPoolForAdaptive<>(maxByteCapacityOfSubIndex, serializer, copyNum, rowNum));
        }
        //设置当前活动子索引
        activeSubIndexPosition = 0;
        System.out.println("当前使用的是基于堆外内存的内存池-JoinerLinkedIndexListOfOffHeapPool,且该子索引与自适应能力对应！");
    }

    public int getMaxSubIndexNum() {
        return maxSubIndexNum;
    }

    public long getMaxByteCapacityOfSubIndex() {
        return maxByteCapacityOfSubIndex;
    }

    public long getTimeIntervalOfSubIndex() {
        return timeIntervalOfSubIndex;
    }

    /**
     * 插入一个新的元组，若插入的新元组导致当前子索引时间范围超过设置的子索引时间范围，更新活动子索引
     * @param key 元组键值
     * @param value 元组的值
     */
    public void insertNewTuple(K key, CommonJoinUnionType<F, S> value) {
        subIndexLinkedList.get(activeSubIndexPosition).insertNewTuple(key, value);
        // 如果插入新的元组后，子索引时间范围超过预定值，则当前索引归档，下一个索引为活动索引
        if (subIndexLinkedList.get(activeSubIndexPosition).getTimeIntervalOfAllStoredTuples() > timeIntervalOfSubIndex) {
            //新建子索引
            creatNewSubIndex();
        }
    }

    /**
     * 用指定的系统复制数和当前Joiner的行号重置本地的复制数和行号，并立即新建（重置）一个子索引用于存储之后到达的元组
     * 该方法一般在进行自适应时进行调用
     * @param copyNum 新的系统复制数
     * @param rowNum 新的行号
     */
    public void resetCopyAndRowNumAndCreateNewSubIndex(int copyNum, int rowNum) {
        this.copyNum = copyNum;
        this.rowNum = rowNum;
        //新建子索引
        creatNewSubIndex();
    }

    /**
     * 新建（重置）一个子索引，即将下一个子索引作为当前活动子索引
     * 用当前系统的复制数与行号初始化下一个子索引
     */
    private void creatNewSubIndex() {
        //新建（重置）子索引
        activeSubIndexPosition = (activeSubIndexPosition + 1) % maxSubIndexNum;
        //用当前系统的复制数与行号初始化下一个子索引
        subIndexLinkedList.get(activeSubIndexPosition).resetWithSpecialSerialRange(copyNum, rowNum);
    }

    /**
     * 该方法一般不被调用
     * 找出在所有子索引中存储的所有键值在指定范围内，且时间戳大于等于所要求的最小时间戳的元组
     * @param fromKey 索要查找的最小键值
     * @param fromInclusive 最小键值对应的元组是否包含在结果中（若为True，则包含）
     * @param toKey 索要查找的最大键值
     * @param toInclusive 最大键值对应的元组是否包含在结果中（若为True，则包含）
     * @param minTimestamp 要查找的最小时间戳，结果中的所有元组的时间戳均大于等于该值
     * @return 所有满足键值范围和时间戳范围的元组组成的列表
     */
    private List<CommonJoinUnionType<F, S>> findRangeCeilingMinTimestamp(K fromKey, boolean fromInclusive, K toKey, boolean toInclusive, long minTimestamp){
        LinkedList<CommonJoinUnionType<F, S>> resultList = new LinkedList<>();

        for (JoinerSubIndexWithOffHeapPoolForAdaptive<F, S, K> subIndex : subIndexLinkedList) {
            if (subIndex.getCurrentStoreTupleNum() == 0) {  //跳过空子索引
                continue;
            }
            if (subIndex.getMaxTimestamp() < minTimestamp) {  //跳过时间不满足的子索引
                continue;
            }
            // 遍历所有符合条件的子树，收集所有结果
            List<CommonJoinUnionType<F, S>> subResultList =
                    subIndex.findRangeCeilingMinTimestamp(fromKey, fromInclusive, toKey, toInclusive, minTimestamp);
            if (subResultList != null) {
                resultList.addAll(subResultList);
            }
        } // end for

        return resultList;
    }

    /**
     * 找出在所有子索引中存储的所有键值在指定范围内，且时间戳大于等于所要求的最小时间戳的元组，
     * 同时序列号也要满足对应子索引的要求，这是为了自适应时更改子索引结构
     * @param fromKey 索要查找的最小键值
     * @param fromInclusive 最小键值对应的元组是否包含在结果中（若为True，则包含）
     * @param toKey 索要查找的最大键值
     * @param toInclusive 最大键值对应的元组是否包含在结果中（若为True，则包含）
     * @param minTimestamp 要查找的最小时间戳，结果中的所有元组的时间戳均大于等于该值
     * @param serialNum 要查询元组的序列号，该元组只与满足序列号要求的子索引进行连接
     * @return 所有满足键值范围和时间戳范围的元组组成的列表
     */
    public List<CommonJoinUnionType<F, S>> findRangeCeilingMinTimestampWitSerial(K fromKey, boolean fromInclusive, K toKey, boolean toInclusive, long minTimestamp, long serialNum) {
        LinkedList<CommonJoinUnionType<F, S>> resultList = new LinkedList<>();

        for (JoinerSubIndexWithOffHeapPoolForAdaptive<F, S, K> subIndex : subIndexLinkedList) {
            if (subIndex.getCurrentStoreTupleNum() == 0) {  //跳过空子索引
                continue;
            }
            if (subIndex.getMaxTimestamp() < minTimestamp) {  //跳过时间不满足的子索引
                continue;
            }
            if (!subIndex.isCurrentSerialNumMatchLocal(serialNum)) {  // 跳过不满足序列号要求的子索引
                continue;
            }
            // 遍历所有符合条件的子树，收集所有结果
            List<CommonJoinUnionType<F, S>> subResultList =
                    subIndex.findRangeCeilingMinTimestamp(fromKey, fromInclusive, toKey, toInclusive, minTimestamp);
            if (subResultList != null) {
                resultList.addAll(subResultList);
            }
        } // end for

        return resultList;
    }

    /**
     * 对所有子索引执行过期操作，重置所有最大时间戳小于指定时间戳的子树
     * @param currentMinStoreTimestamp 所有最大时间戳小于该值的子索引都将被重置
     */
    public void expire(long currentMinStoreTimestamp) {
        for (JoinerSubIndexWithOffHeapPoolForAdaptive<F, S, K> subIndex : subIndexLinkedList) {
            if ((subIndex.getCurrentStoreTupleNum() != 0) && (subIndex.getMaxTimestamp() < currentMinStoreTimestamp)) {
                subIndex.resetWithSpecialSerialRange(copyNum, rowNum);
            }
        }
    }

    /**
     * 获取当前所有子索引中存储的子树数量以及总的存储元组的数量
     *
     * @return 一个二元组，其结构为 -子树数量，总元组数量-
     */
    public Tuple2<Long, Long> getIndexLinkedListStoreStatus(){
        //所有存储元组的子索引数量以及所有已经存储的元组数量
        long totalSubIndexNum = 0;
        long totalStoredTupleNum = 0;
        for (JoinerSubIndexWithOffHeapPoolForAdaptive<F, S, K> subIndex : subIndexLinkedList){
            if (subIndex.getCurrentStoreTupleNum() != 0) {
                totalSubIndexNum++;
                totalStoredTupleNum += subIndex.getCurrentStoreTupleNum();
            }
        }
        return new Tuple2<>(totalSubIndexNum, totalStoredTupleNum);
    }

    /**
     * 获取相对于当前活动子索引指定位置偏移的子索引的内存占用（单位：字节）
     * @param offset 相对于当前活动子索引的偏移，当前活动子索引为 0，上一个归档的子索引为 1
     * @return 对应偏移位置的子索引的内存占用（单位：字节）
     */
    public long getMemoryFootprintOffsetCurrent(int offset) {
        if (activeSubIndexPosition - offset >= 0) {
            return subIndexLinkedList.get(activeSubIndexPosition - offset).getCurrentInsertPositionOffset();
        } else {
            return subIndexLinkedList.get(maxSubIndexNum + activeSubIndexPosition - offset).getCurrentInsertPositionOffset();
        }

    }

    /**
     * 获取所有子索引总共的内存占用
     * @return 所有子索引总共的内存占用
     */
    public long getTotalMemoryFootprint() {
        long totalMemoryFootprint = 0;
        for (JoinerSubIndexWithOffHeapPoolForAdaptive<F, S, K> subIndex : subIndexLinkedList){
            totalMemoryFootprint += subIndex.getCurrentInsertPositionOffset();
        }
        return totalMemoryFootprint;
    }

    /**
     * 依次返回所有子索引的统计信息
     *
     * @return 六元组列表：<已存储元组数，已存储字节数，最大字节容量，当前最大时间戳，对应复制数，对应行号>
     */
    public List<Tuple6<Integer, Long, Long, Long, Integer, Integer>> getStoreStatusOfAllSubIndex() {
        ArrayList<Tuple6<Integer, Long, Long, Long, Integer, Integer>> resultList = new ArrayList<>(maxSubIndexNum);
        for (JoinerSubIndexWithOffHeapPoolForAdaptive<F, S, K> subIndex : subIndexLinkedList) {
            resultList.add(new Tuple6<>(
                    subIndex.getCurrentStoreTupleNum(),
                    subIndex.getCurrentInsertPositionOffset(),
                    subIndex.getMaxCapacity(),
                    subIndex.getMaxTimestamp(),
                    subIndex.getCopyNum(),
                    subIndex.getRowNum()));
        }
        return resultList;
    }

    /**
     * 销毁内部所有结构
     */
    public void destroy(){
        for (JoinerSubIndexWithOffHeapPoolForAdaptive<F, S, K> subIndex : subIndexLinkedList){
            subIndex.destroy();
        }
        subIndexLinkedList.clear();
    }

}
