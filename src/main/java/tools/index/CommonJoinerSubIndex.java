package tools.index;

import java.io.Serializable;
import java.util.List;

/**
 * 如果Joiner中采用的是链式索引结构，则该类表示的是其中的每个子索引的类型
 * @param <K> 键值类型
 * @param <V> 键值对应的元组类型
 */
public abstract class CommonJoinerSubIndex<K extends Comparable<K>, V> implements Serializable {
    /**
     * 返回该子索引中包含的项的数量
     *
     * @return 该子索引中包含的项的数量
     */
    public abstract long getLength();

    /**
     * 根据指定的条件删除叶子节点
     * 便利所有叶子节点，删除其中所有满足条件的节点
     *
     * @param condition 要删除的节点满足的条件
     */
    public abstract void deleteNodeWithCondition(DeleteConditionForSubIndex<V> condition);

    /**
     * 向该子索引中插入一个元组
     * @param key 元组对应的键值
     * @param value 元组的具体值
     * @param timestamp 元组的时间戳
     */
    public abstract void insert(K key, V value, long timestamp);

    /**
     * 返回该子索引中存储的所有元组的时间戳中的最大值
     * @return 该子索引中存储的所有元组的时间戳中的最大值
     */
    public abstract long getMaxTimestamp();

    /**
     * 返回该子索引中存储的所有元组的时间戳中的最小值
     * @return 该子索引中存储的所有元组的时间戳中的最小值
     */
    public abstract long getMinTimestamp();

    /**
     * 找出在该子索引中存储的所有键值在指定范围内，且时间戳大于等于所要求的最小时间戳的元组
     * @param fromKey 索要查找的最小键值
     * @param fromInclusive 最小键值对应的元组是否包含在结果中（若为True，则包含）
     * @param toKey 索要查找的最大键值
     * @param toInclusive 最大键值对应的元组是否包含在结果中（若为True，则包含）
     * @param minTimestamp 要查找的最小时间戳，结果中的所有元组的时间戳均大于等于该值
     * @return 所有满足键值范围和时间戳范围的元组组成的列表
     */
    public abstract List<V> findRangeCeilingMinTimestamp(K fromKey, boolean fromInclusive, K toKey, boolean toInclusive, long minTimestamp);


    /**
     * 清除当前子索引中的所有元组
     */
    public abstract void clear();

}
