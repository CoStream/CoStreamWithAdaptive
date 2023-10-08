package tools.index;

import base.subTreeForCoModel.DeleteConditionForSubTree;
import base.subTreeForCoModel.SubBPlusTreeForCoModel;

import java.util.List;

/**
 * 基于B+树实现的子索引结构
 * @param <K> 子索引中存储的元组的键值类型
 * @param <V> 子索引中存储的元组类型
 */
public class JoinerSubIndexWithBPlusTree<K extends Comparable<K>, V> extends CommonJoinerSubIndex<K, V> {

    // 本地实际用于存储元组的B+树
    private SubBPlusTreeForCoModel<V, K> localBPlusTree;

    public JoinerSubIndexWithBPlusTree() {
        localBPlusTree = new SubBPlusTreeForCoModel<>();
    }

    @Override
    public long getLength() {
        return localBPlusTree.getLength();
    }

    @Override
    public void deleteNodeWithCondition(DeleteConditionForSubIndex<V> condition) {
        localBPlusTree.deleteNodeWithCondition(new DeleteConditionForSubTree<V>() {
            @Override
            public boolean isDeleted(V tuple) {
                return condition.isDeleted(tuple);
            }
        });
    }

    @Override
    public void insert(K key, V value, long timestamp) {
        localBPlusTree.insert(value, key, timestamp);
    }

    @Override
    public long getMaxTimestamp() {
        return localBPlusTree.getMaxTimestamp();
    }

    @Override
    public long getMinTimestamp() {
        return localBPlusTree.getMinTimestamp();
    }

    /**
     * 要查找的范围两端都包含，此时 fromInclusive 与 toInclusive 无效
     */
    @Override
    public List<V> findRangeCeilingMinTimestamp(K fromKey, boolean fromInclusive, K toKey, boolean toInclusive, long minTimestamp) {
        return localBPlusTree.findRange(fromKey, toKey, minTimestamp);
    }

    @Override
    public void clear() {
        localBPlusTree.clear();
    }
}
