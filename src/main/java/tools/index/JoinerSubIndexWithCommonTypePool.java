package tools.index;

import base.CommonJoinUnionType;

import java.io.Serializable;
import java.util.*;


/**
 * 对 CommonJoinUnionType 进行池化的子索引结构
 * @param <K> : 存储的键值类型
 */
public class JoinerSubIndexWithCommonTypePool<F, S, K extends Comparable<K>> implements Serializable {
    //该子索引存储元组数量的上限
    protected int maxSize;
    //当前插入元组的位置（即下一个插入的元组就插入到该位置）
    protected int currentInsertPosition;
    //存储所有数据的数组，也就是内存池
    protected CommonJoinUnionType[] localSubIndexArray;
    //存储索引
    protected TreeMap<K, List<Integer>> searchIndex = new TreeMap<>();
    //保存在该子索引中的最大和最小时间戳
    protected long maxTimestamp;
    protected long minTimestamp;

    public JoinerSubIndexWithCommonTypePool(int maxSize) {
        this.maxSize = maxSize;

        //重置本地时间戳
        maxTimestamp = Long.MIN_VALUE;
        minTimestamp = Long.MAX_VALUE;

        //当前插入位置为0
        currentInsertPosition = 0;

        //初始化本地所有的节点
        localSubIndexArray = new CommonJoinUnionType[maxSize];
        for (int i = 0; i < maxSize; i++) {
            localSubIndexArray[i] = new CommonJoinUnionType<F, S>();
        }
    }

    /**
     * 简单的将一个元组插入到本地存储结构中，不涉及索引的操作，并返回插入元组存储的位置
     * 该方法仅仅是将currentInsertPosition指定的位置的元组的值复制为要插入的元组，并currentInsertPosition加一
     * @param value 要插入的元组
     * @return 新插入的元组在本地的存储位置
     */
    protected int simpleInsertOneTupleToLocalIndex(CommonJoinUnionType<F, S> value) {
        localSubIndexArray[currentInsertPosition].copyFrom(value);
        currentInsertPosition++;
        return currentInsertPosition - 1;
    }

    /**
     * 向本地插入一个新的元组(插入的元组内部必须自己包含正确的时间戳，否则查找时无法获取时间戳)
     * 该方法其实是将插入的元组的所有属性复制一份放在本地索引中，因此原元组是否清空不影响插入的元组
     * 此方法会更新本地的最大和最小时间戳
     * @param key 元组的键值
     * @param value 元组的值
     */
    public void insertNewTuple(K key, CommonJoinUnionType<F, S> value) {
        //插入索引
        int position = simpleInsertOneTupleToLocalIndex(value);
        //更新搜索索引
        if (searchIndex.containsKey(key)) {
            searchIndex.get(key).add(position);
        } else {
            LinkedList<Integer> newList = new LinkedList<>();
            newList.add(position);
            searchIndex.put(key, newList);
        }
        //更新最大和最小时间戳
        if (value.getSelfTimestamp() > maxTimestamp) {
            maxTimestamp = value.getSelfTimestamp();
        }
        if (value.getSelfTimestamp() < minTimestamp) {
            minTimestamp = value.getSelfTimestamp();
        }
    }

    public long getMaxTimestamp() {
        return maxTimestamp;
    }

    public long getMinTimestamp() {
        return minTimestamp;
    }

    /**
     * 获取该子索引存储的所有元组的时间戳的范围（即最大时间戳和最小时间戳的差）
     */
    public long getTimeIntervalOfAllStoredTuples() {
        if (getSize() == 0) {
            return 0;
        }
        return maxTimestamp - minTimestamp;
    }

    /**
     * 获取当前已经存储的元组数量
     * @return 当前该子索引中存储的元组数量
     */
    public int getSize() {
        return currentInsertPosition;
    }

    /**
     * 获取该子索引的最大容量
     * @return 当前子索引的最大容量
     */
    public int getMaxCapacity() {
        return maxSize;
    }

    /**
     * 获取指定位置的元组
     * @param position 要获取的元组的位置
     * @return 获取的元组
     */
    private CommonJoinUnionType<F, S> getTupleInSpecificPosition(int position) {
        return (CommonJoinUnionType<F, S>)localSubIndexArray[position];
    }

    /**
     * 找出在该子索引中存储的所有键值在指定范围内，且时间戳大于等于所要求的最小时间戳的元组
     * @param fromKey 索要查找的最小键值
     * @param fromInclusive 最小键值对应的元组是否包含在结果中（若为True，则包含）
     * @param toKey 索要查找的最大键值
     * @param toInclusive 最大键值对应的元组是否包含在结果中（若为True，则包含）
     * @param minTimestamp 要查找的最小时间戳，结果中的所有元组的时间戳均大于等于该值
     * @return 所有满足键值范围和时间戳范围的元组组成的列表
     */
    public List<CommonJoinUnionType<F, S>> findRangeCeilingMinTimestamp(K fromKey, boolean fromInclusive, K toKey, boolean toInclusive, long minTimestamp) {
        //获取所有键值在指定范围内的元组的位置
        NavigableMap<K, List<Integer>> resultSearchMap = searchIndex.subMap(fromKey, fromInclusive, toKey, toInclusive);
        Set<Map.Entry<K, List<Integer>>> entries = resultSearchMap.entrySet();
        //构建结果列表
        LinkedList<CommonJoinUnionType<F, S>> resultList = new LinkedList<>();
        //遍历所有指定位置的元组，若对应元组的时间戳大于等于给定的时间戳，则将其加入到结果中
        for (Map.Entry<K, List<Integer>> e : entries) {
            for (Integer position : e.getValue()) {
                if (getTupleInSpecificPosition(position).getSelfTimestamp() >= minTimestamp) {
                    resultList.add(getTupleInSpecificPosition(position));
                }
            }
        }
        //返回结果列表
        return resultList;
    }


    /**
     * 重置当前子树（之后可以向该子树中重新插入元组，相当于删除当前子树，但实际本地的存储结构未做任何改变）
     */
    public void reset() {
        //重置读取位置
        currentInsertPosition = 0;
        //重置本地时间戳
        maxTimestamp = Long.MIN_VALUE;
        minTimestamp = Long.MAX_VALUE;

        searchIndex.clear();
    }
}
