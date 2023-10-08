package tools.index;

import base.CommonJoinUnionType;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * 由所有元组级别的内存池子索引构成的子索引列表
 * @param <F> 第一个流的类型
 * @param <S> 第二个流的类型
 * @param <K> 存储的键值类型
 */
public class JoinerLinkedIndexListOfCommonTypePool<F, S, K extends Comparable<K>> implements Serializable {
    //最大子索引数量
    private int maxSubIndexNum;
    //每个子索引的最大容量
    private int maxCapacityOfSubIndex;
    //每个子索引存储元组的时间范围
    private long timeIntervalOfSubIndex;
    //子索引链表
    private List<JoinerSubIndexWithCommonTypePool<F, S, K>> subIndexLinkedList;
    //当前活动子索引位置
    private int activeSubIndexPosition;

    /**
     * @param maxSubIndexNum 最大子索引数量
     * @param maxCapacityOfSubIndex 每个子索引的最大容量
     */
    public JoinerLinkedIndexListOfCommonTypePool(int maxSubIndexNum, int maxCapacityOfSubIndex, long timeIntervalOfSubIndex) {
        this.maxSubIndexNum = maxSubIndexNum;
        this.maxCapacityOfSubIndex = maxCapacityOfSubIndex;
        this.timeIntervalOfSubIndex = timeIntervalOfSubIndex;

        // 初始化所有子索引
        subIndexLinkedList = new ArrayList<>(maxSubIndexNum);
        for (int i = 0; i < maxSubIndexNum; i++) {
            subIndexLinkedList.add(new JoinerSubIndexWithCommonTypePool<F, S, K>(maxCapacityOfSubIndex));
        }

        //设置当前活动子索引
        activeSubIndexPosition = 0;
    }

    public int getMaxSubIndexNum() {
        return maxSubIndexNum;
    }

    public int getMaxCapacityOfSubIndex() {
        return maxCapacityOfSubIndex;
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
        if (subIndexLinkedList.get(activeSubIndexPosition).getTimeIntervalOfAllStoredTuples() > timeIntervalOfSubIndex) {
            activeSubIndexPosition = (activeSubIndexPosition + 1) % maxSubIndexNum;
        }
    }

    /**
     * 找出在所有子索引中存储的所有键值在指定范围内，且时间戳大于等于所要求的最小时间戳的元组
     * @param fromKey 索要查找的最小键值
     * @param fromInclusive 最小键值对应的元组是否包含在结果中（若为True，则包含）
     * @param toKey 索要查找的最大键值
     * @param toInclusive 最大键值对应的元组是否包含在结果中（若为True，则包含）
     * @param minTimestamp 要查找的最小时间戳，结果中的所有元组的时间戳均大于等于该值
     * @return 所有满足键值范围和时间戳范围的元组组成的列表
     */
    public List<CommonJoinUnionType<F, S>> findRangeCeilingMinTimestamp(K fromKey, boolean fromInclusive, K toKey, boolean toInclusive, long minTimestamp){
        LinkedList<CommonJoinUnionType<F, S>> resultList = new LinkedList<>();

        for (JoinerSubIndexWithCommonTypePool<F, S, K> subIndex : subIndexLinkedList) {
            if (subIndex.getSize() == 0) {  //跳过空子索引
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
     * 对所有子索引执行过期操作，重置所有最大时间戳小于指定时间戳的子树
     * @param currentMinStoreTimestamp 所有最大时间戳小于该值的子索引都将被重置
     */
    public void expire(long currentMinStoreTimestamp) {
        for (JoinerSubIndexWithCommonTypePool<F, S, K> subIndex : subIndexLinkedList) {
            if ((subIndex.getSize() != 0) && (subIndex.getMaxTimestamp() < currentMinStoreTimestamp)) {
                subIndex.reset();
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
        for (JoinerSubIndexWithCommonTypePool<F, S, K> subIndex : subIndexLinkedList){
            if (subIndex.getSize() != 0) {
                totalSubIndexNum++;
                totalStoredTupleNum += subIndex.getSize();
            }
        }
        return new Tuple2<>(totalSubIndexNum, totalStoredTupleNum);
    }

    /**
     * 依次返回所有子索引的统计信息
     * @return 三元组列表：<已存储元组数，最大容量，当前最大时间戳>
     */
    public List<Tuple3<Integer,Integer,Long>> getStoreStatusOfAllSubIndex() {
        ArrayList<Tuple3<Integer, Integer, Long>> resultList = new ArrayList<>(maxSubIndexNum);
        for (JoinerSubIndexWithCommonTypePool<F, S, K> subIndex : subIndexLinkedList){
            resultList.add(new Tuple3<>(subIndex.getSize(), subIndex.getMaxCapacity(), subIndex.getMaxTimestamp()));
        }
        return resultList;
    }

}
