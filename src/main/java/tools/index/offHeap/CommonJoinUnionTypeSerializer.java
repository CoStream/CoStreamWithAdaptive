package tools.index.offHeap;

import base.CommonJoinUnionType;

import java.io.Serializable;

/**
 * 对于基于堆外内存的CommonJoinUnionType的序列化器和反序列化器
 * @param <F> 第一个流的类型
 * @param <S> 第二个流的类型
 */
public interface CommonJoinUnionTypeSerializer<F, S> extends Serializable {
    /**
     * 将元组序列化到堆外内存的指定初始偏移量位置
     * @param value 要序列化的元组
     * @param initOff 堆外内存的初始偏移量
     * @return 插入字节的长度
     */
    abstract public int serializeToOffHeap(CommonJoinUnionType<F, S> value, long initOff);

    /**
     * 将指定偏移量，指定长度的元组反序列化
     * @param initOff 要反序列化的初始偏移
     * @param length 要反序列化的长度(暂时用不上)
     * @return 反序列化的元组
     */
    abstract public CommonJoinUnionType<F, S> deserializeFromOffHeap(long initOff, long length);


    /**
     * 获取指定偏移位置的元组的时间戳，方便过滤不满足时间条件的元组
     * @param tupleOff 元组的起始偏移
     * @return 元组对应的自己的时间戳
     */
    abstract public long getTimestampInSpecificPosition(long tupleOff);
}
