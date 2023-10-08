package tools.index.offHeap;

import base.CommonJoinUnionType;
import org.apache.flink.core.memory.MemoryUtils;
import sun.misc.Unsafe;

import java.io.Serializable;
import java.util.*;


/**
 * 采用堆外内存作为内存池的子索引结构
 * @param <K> : 存储的键值类型
 */
public class JoinerSubIndexWithOffHeapPool<F, S, K extends Comparable<K>> implements Serializable {
    //该子索引存储元组数量的上限（单位：字节数）
    protected long maxSize;
    //当前插入元组的位置（即下一个插入的元组就插入到该位置）
    protected long currentInsertPosition;
    //存储索引
    protected TreeMap<K, List<Long>> searchIndex = new TreeMap<>();
    //保存在该子索引中的最大和最小时间戳
    protected long maxTimestamp;
    protected long minTimestamp;
    //用于堆外内存的序列化器
    CommonJoinUnionTypeSerializer<F, S> serializer;

    // 堆外内存分配器以及分配的初始地址
    private static final Unsafe unsafe = MemoryUtils.UNSAFE;
    private long initAddress;

    //当前存储的元组数
    private int currentStoreTupleNum;

    /**
     * 初始化
     * @param maxSize 该子索引存储元组数量的上限（单位：字节数）
     * @param serializer 元组堆外内存序列化器
     */
    public JoinerSubIndexWithOffHeapPool(long maxSize, CommonJoinUnionTypeSerializer<F, S> serializer) {
        this.maxSize = maxSize;
        this.serializer = serializer;
        initAddress = unsafe.allocateMemory(maxSize);
        reset();
    }

    /**
     * 简单的将一个元组插入到本地存储结构中，不涉及索引的操作，并返回插入元组存储的位置
     * 该方法仅仅是将currentInsertPosition指定的位置的元组的值复制为要插入的元组，并currentInsertPosition加一
     * @param value 要插入的元组
     * @return 新插入的元组在本地的存储位置
     */
    protected long simpleInsertOneTupleToLocalIndex(CommonJoinUnionType<F, S> value) {
        int length = serializer.serializeToOffHeap(value, currentInsertPosition);
        currentInsertPosition += length;
        return currentInsertPosition - length;
    }

    /**
     * 向本地插入一个新的元组(插入的元组内部必须自己包含正确的时间戳，否则查找时无法获取时间戳)
     * 该方法其实是将插入的元组的所有属性复制一份放在本地索引中，因此原元组是否清空不影响插入的元组
     * 此方法会更新本地的最大和最小时间戳
     * @param key 元组的键值
     * @param value 元组的值
     */
    public void insertNewTuple(K key, CommonJoinUnionType<F, S> value) {
        long position = simpleInsertOneTupleToLocalIndex(value);
        currentStoreTupleNum++;
        updateSearchIndexAndTimeInterval(key, position, value);
    }

    /**
     * 新插入元组时更新本地搜索索引以及时间范围
     * @param key      新插入元组的键值
     * @param position 新插入元组的位置
     * @param value 要插入的元组
     */
    protected void updateSearchIndexAndTimeInterval(K key, long position, CommonJoinUnionType<F, S> value) {
        //更新搜索索引
        if (searchIndex.containsKey(key)) {
            searchIndex.get(key).add(position);
        } else {
            LinkedList<Long> newList = new LinkedList<>();
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
        if (getCurrentStoreTupleNum() == 0) {
            return 0;
        }
        return maxTimestamp - minTimestamp;
    }

    /**
     * 获取当前已经存储的元组数量
     * @return 当前该子索引中存储的元组数量
     */
    public int getCurrentStoreTupleNum() {
        return currentStoreTupleNum;
    }

    /**
     * 获取当前在内存中的插入位置偏移，表示在已经申请的内存已经使用了多少内存
     * @return 在已经申请的内存已经使用了多少内存
     */
    public long getCurrentInsertPositionOffset() {
        return currentInsertPosition - initAddress;
    }

    /**
     * 获取该子索引的最大容量(字节数)
     * @return 当前子索引的最大容量（字节数）
     */
    public long getMaxCapacity() {
        return maxSize;
    }

    /**
     * 获取指定位置的元组
     * @param position 要获取的元组的位置
     * @return 获取的元组
     */
    private CommonJoinUnionType<F, S> getTupleInSpecificPosition(long position) {
        return serializer.deserializeFromOffHeap(position, 0);  //长度暂时用不上，目前置为0
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
        NavigableMap<K, List<Long>> resultSearchMap = searchIndex.subMap(fromKey, fromInclusive, toKey, toInclusive);
        Set<Map.Entry<K, List<Long>>> entries = resultSearchMap.entrySet();
        //构建结果列表
        LinkedList<CommonJoinUnionType<F, S>> resultList = new LinkedList<>();
        //遍历所有指定位置的元组，若对应元组的时间戳大于等于给定的时间戳，则将其加入到结果中
        for (Map.Entry<K, List<Long>> e : entries) {
            for (Long position : e.getValue()) {  // 所有键值在指定范围内的元组的位置
                if (serializer.getTimestampInSpecificPosition(position) >= minTimestamp) {
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
        currentInsertPosition = initAddress;
        //重置本地时间戳
        maxTimestamp = Long.MIN_VALUE;
        minTimestamp = Long.MAX_VALUE;

        //重置查询索引
        searchIndex.clear();

        //当前存储的元组数量清零
        currentStoreTupleNum = 0;
    }

    /**
     * 彻底删除当前索引，销毁所有内部结构，一般只有在程序运行结束时才调用
     */
    public void destroy() {
        searchIndex.clear();
        unsafe.freeMemory(initAddress);
    }
}
