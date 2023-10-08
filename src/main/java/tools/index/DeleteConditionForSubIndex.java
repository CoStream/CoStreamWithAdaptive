package tools.index;

/**
 * 用于判断B+树中的某个叶子节点是否应该删除的接口，主要用于CoModel中进行根据序列号的叶子节点删除
 * @param <T> B+树中存储的节点类型
 */
public interface DeleteConditionForSubIndex<T> {

    /**
     * 判断一个元组是否应该被删除，若返回true，则应该被删除
     * @param tuple 要进行判断的元组
     * @return true：该元组应该被删除；
     */
    public boolean isDeleted(T tuple);
}
