package tools.index;

import base.CommonJoinUnionType;
import org.apache.flink.api.java.tuple.Tuple2;

import java.io.*;
import java.util.*;

/**
 * 采用字节数组作为内存池的子索引结构
 * @param <K> 键值
 */
public class JoinerSubIndexWithBytePool<F, S, K extends Comparable<K>> implements Serializable {
    //该子索引存储字节元组数量的上限(单位：字节)
    private int maxCapacity;
    //当前插入元组的位置（即下一个插入的元组就插入到该位置）
    private int currentInsertPosition;
    //已经存储的元组数量
    private int currentStoredTupleNum;
    //存储所有数据的数组，也就是内存池
    private byte[] localByteArray;
    //存储索引(保存每个键值在字节数组中的-起始位置，长度-)
    private TreeMap<K, List<Tuple2<Integer,Integer>>> searchIndex = new TreeMap<>();
    //保存在该子索引中的最大和最小时间戳
    private long maxTimestamp;
    private long minTimestamp;

    /**
     * 构造方法
     * @param maxCapacity 当前字节数组内存池的最大容量(单位：字节)
     */
    public JoinerSubIndexWithBytePool(int maxCapacity) {
        this.maxCapacity = maxCapacity;
        //初始化相关结构
        localByteArray = new byte[maxCapacity];
        reset();
    }

    /**
     * 向本地插入一个新的元组(插入的元组内部必须自己包含正确的时间戳，否则查找时无法获取时间戳)
     * 该方法其实是将插入的元组序列化后放在本地字节数组中
     * 此方法会更新本地的最大和最小时间戳
     * @param key 元组的键值
     * @param value 元组的值
     */
    public void insertNewTuple(K key, CommonJoinUnionType<F, S> value){
        //序列化到本地字节数组
        Tuple2<Integer, Integer> positionTuple2 = serializeTupleToLocalIndex(value);
        //更新搜索索引
        if (searchIndex.containsKey(key)) {
            searchIndex.get(key).add(positionTuple2);
        } else {
            LinkedList<Tuple2<Integer, Integer>> newList = new LinkedList<>();
            newList.add(positionTuple2);
            searchIndex.put(key, newList);
        }
        //更新已存储元组数量
        currentStoredTupleNum++;
        //更新最大和最小时间戳
        if (value.getSelfTimestamp() > maxTimestamp) {
            maxTimestamp = value.getSelfTimestamp();
        }
        if (value.getSelfTimestamp() < minTimestamp) {
            minTimestamp = value.getSelfTimestamp();
        }
    }


    /**
     * 简单的将一个元组插入到本地存储结构中，不涉及索引的操作，并返回插入元组的 -起始位置，长度-
     * @param value 要插入的元组
     * @return 新插入的元组在本地的 -起始位置，长度-
     */
    private Tuple2<Integer, Integer> serializeTupleToLocalIndex(CommonJoinUnionType<F, S> value) {
        //插入的起始位置
        int startPosition = currentInsertPosition;

        MyZeroCopyOutputStream bos = new MyZeroCopyOutputStream();
        ObjectOutputStream oos = null;

        try {
            oos = new ObjectOutputStream(bos);
            oos.writeObject(value);  // 写入对象
        } catch (IOException e) {
            e.printStackTrace();
            System.err.println("JoinerSubIndexWithBytePool中插入元组序列化失败！");
        }

        //写入的对象长度
        int writeSize = bos.size();

        //将写入的对象的字节数组复制到本地字节数组的指定初始位置
        bos.myWrite(localByteArray, currentInsertPosition);

        //更新下一个写入位置
        currentInsertPosition += writeSize;

        //关闭流
        try {
            bos.close();
            if (oos != null) {
                oos.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.err.println("JoinerSubIndexWithBytePool中插入元组序列化失败！(输出流未能关闭)");
        }

        return new Tuple2<>(startPosition, writeSize);
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
        NavigableMap<K, List<Tuple2<Integer, Integer>>> resultSearchMap = searchIndex.subMap(fromKey, fromInclusive, toKey, toInclusive);
        Set<Map.Entry<K, List<Tuple2<Integer, Integer>>>> entries = resultSearchMap.entrySet();
        //构建结果列表
        LinkedList<CommonJoinUnionType<F, S>> resultList = new LinkedList<>();
        //遍历所有指定位置的元组，若对应元组的时间戳大于等于给定的时间戳，则将其加入到结果中
        for (Map.Entry<K, List<Tuple2<Integer, Integer>>> e : entries) {
            for (Tuple2<Integer, Integer> position : e.getValue()) {
                CommonJoinUnionType<F, S> resultTuple = deserializeTupleInSpecificPosition(position);
                if (resultTuple.getSelfTimestamp() >= minTimestamp) {
                    resultList.add(resultTuple);
                }
            }
        }
        //返回结果列表
        return resultList;
    }

    /**
     * 将位置元组指定位置的元组反序列化并返回
     * @param positionTuple2 要查找元组的 -起始位置，长度-
     * @return 指定位置的元组
     */
    private CommonJoinUnionType<F, S> deserializeTupleInSpecificPosition(Tuple2<Integer, Integer> positionTuple2) {

        CommonJoinUnionType<F, S> resultTuple = null;

        try {
            ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(localByteArray, positionTuple2.f0, positionTuple2.f1));
            resultTuple = (CommonJoinUnionType<F, S>)ois.readObject();
            ois.close();
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
            System.err.println("JoinerSubIndexWithBytePool中查找元组反序列化失败！");
        }

        return resultTuple;
    }

    /**
     * 获取已经存储的元组数量
     */
    public int getSize() {
        return currentStoredTupleNum;
    }

    /**
     * 获取已经存储的字节数
     */
    public int getByteSize() {
        return currentInsertPosition;
    }

    public int getMaxCapacity() {
        return maxCapacity;
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
     * 重置当前子索引，相当于清空，但是实际上不会对底层字节数组产生影响
     */
    public void reset() {
        currentInsertPosition = 0;
        searchIndex.clear();
        maxTimestamp = Long.MIN_VALUE;
        minTimestamp = Long.MAX_VALUE;
        currentStoredTupleNum = 0;
    }



    /**
     * 重写的字节数组输出流类，添加了直接将输出流中的字节数组复制到本地字节数组池的方法，减少了原有方法中的一次冗余复制
     */
    private static class MyZeroCopyOutputStream extends ByteArrayOutputStream implements Serializable{

        /**
         * 直接将输出流中的字节数组复制到本地字节数组池的方法，减少了原有方法中的一次冗余复制
         * @param in 要接收流中字节数组的本地字节数组池
         * @param offset 本地字节数组池中的写入初始偏移
         */
        public void myWrite(byte[] in, int offset) {
            // 原数组，原起始位置，目的数组，目的起始位置，写入长度
            System.arraycopy(buf, 0, in, offset, count);
        }

    }
}
