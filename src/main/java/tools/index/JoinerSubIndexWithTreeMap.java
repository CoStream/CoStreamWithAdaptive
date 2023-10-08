package tools.index;

import org.apache.flink.api.java.tuple.Tuple2;

import java.util.*;

/**
 * 基于TreeMap实现的子索引结构
 * @param <K> 子索引中存储的元组的键值类型
 * @param <V> 子索引中存储的元组类型
 */
public class JoinerSubIndexWithTreeMap<K extends Comparable<K>, V> extends CommonJoinerSubIndex<K, V>{

    // 实际用于存储的索引结构(此处由于键值不能有重复，因而只能用列表)
    private TreeMap<K, List<Tuple2<Long, V>>> localTreeMap;
    // 该索引中存储的所有元组的时间戳的最小值和最大值
    private long localMinTimestamp;
    private long localMaxTimestamp;
    //该索引中存储的元组数量
    private long localLength;


    /**
     * 所有值初始化，最大值初始为最小，最小值初始为最大
     */
    public JoinerSubIndexWithTreeMap() {
        localTreeMap = new TreeMap<>();
        localMinTimestamp = Long.MAX_VALUE;
        localMaxTimestamp = Long.MIN_VALUE;
        localLength = 0;
    }

    @Override
    public long getLength() {
        return localLength;
    }

    @Override
    public void deleteNodeWithCondition(DeleteConditionForSubIndex<V> condition) {
        Set<Map.Entry<K, List<Tuple2<Long, V>>>> entries = localTreeMap.entrySet();
        Iterator<Map.Entry<K, List<Tuple2<Long, V>>>> iterator = entries.iterator();
        while (iterator.hasNext()) {
            Map.Entry<K, List<Tuple2<Long, V>>> next = iterator.next();
            Iterator<Tuple2<Long, V>> listIterator = next.getValue().iterator();
            while (listIterator.hasNext()) {
                Tuple2<Long, V> listNext = listIterator.next();
                if (condition.isDeleted(listNext.f1)) {
                    listIterator.remove();
                    localLength--;
                }
            }
            if (next.getValue().isEmpty()) {
                iterator.remove();
            }
        }
    }

    @Override
    public void insert(K key, V value, long timestamp) {
        if (localTreeMap.containsKey(key)) {
            localTreeMap.get(key).add(new Tuple2<>(timestamp, value));
        } else {
            LinkedList<Tuple2<Long, V>> newList = new LinkedList<>();
            newList.add(new Tuple2<>(timestamp, value));
            localTreeMap.put(key, newList);
        }

        if (timestamp < localMinTimestamp) {
            localMinTimestamp = timestamp;
        }

        if (timestamp > localMaxTimestamp) {
            localMaxTimestamp = timestamp;
        }

        localLength++;
    }

    @Override
    public long getMaxTimestamp() {
        return localMaxTimestamp;
    }

    @Override
    public long getMinTimestamp() {
        return localMinTimestamp;
    }

    @Override
    public List<V> findRangeCeilingMinTimestamp(K fromKey, boolean fromInclusive, K toKey, boolean toInclusive, long minTimestamp) {

        NavigableMap<K, List<Tuple2<Long, V>>> resultMap = localTreeMap.subMap(fromKey, fromInclusive, toKey, toInclusive);
        Set<Map.Entry<K, List<Tuple2<Long, V>>>> entries = resultMap.entrySet();
        LinkedList<V> resultList = new LinkedList<>();

        for (Map.Entry<K, List<Tuple2<Long, V>>> e : entries) {
            for (Tuple2<Long, V> t : e.getValue()) {
                if (t.f0 >= minTimestamp) {
                    resultList.add(t.f1);
                }
            }
        }
        return resultList;
    }

    @Override
    public void clear() {
        localTreeMap.clear();
        localMinTimestamp = 0;
        localMaxTimestamp = 0;
        localLength = 0;
    }
}
