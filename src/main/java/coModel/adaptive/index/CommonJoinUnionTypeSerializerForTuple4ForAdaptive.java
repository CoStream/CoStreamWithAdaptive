package coModel.adaptive.index;

import base.CommonJoinUnionType;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.core.memory.MemoryUtils;
import sun.misc.Unsafe;

/**
 * 基于Tuple4<String, String, String, String>实现的堆外元组序列化器,对应自适应功能
 */
public class CommonJoinUnionTypeSerializerForTuple4ForAdaptive implements CommonJoinUnionTypeSerializerForAdaptive<Tuple4<String, String, String, String>, Tuple4<String, String, String, String>> {

    private static final Unsafe unsafe = MemoryUtils.UNSAFE;
    //读取字符串时的字符串缓存
    private byte[] cache = new byte[10240];


    @Override
    public int serializeToOffHeap(CommonJoinUnionType<Tuple4<String, String, String, String>, Tuple4<String, String, String, String>> value, long initOff) {

        unsafe.putInt(initOff, value.getNumPartition());  //4字节
        unsafe.putLong(initOff + 4, value.getSelfTimestamp());  //8字节

        if (value.isStoreMode()) {  //1字节
            unsafe.putByte(initOff + 12, (byte) 1);
        }else{
            unsafe.putByte(initOff + 12, (byte) 0);
        }

        if (value.isJoinMode()) {  //1字节
            unsafe.putByte(initOff + 13, (byte) 1);
        } else {
            unsafe.putByte(initOff + 13, (byte) 0);
        }


        unsafe.putInt(initOff + 14, value.getSerialNumOfCoModel());  //4字节
        unsafe.putLong(initOff + 18, value.getOtherTimestamp());  //8字节

        // 之前这些总共26字节
        // 开始写入字符串，此时分为R和S的不同而只写入对应的部分
        if (value.isOne()) {
            unsafe.putByte(initOff + 26, (byte) 1);
            long next1 = insertStringToOffHeap(value.getFirstType().f0, initOff + 27);
            long next2 = insertStringToOffHeap(value.getFirstType().f1, next1);
            long next3 = insertStringToOffHeap(value.getFirstType().f2, next2);
            long next4 = insertStringToOffHeap(value.getFirstType().f3, next3);
            return (int) (next4 - initOff);
        } else {
            unsafe.putByte(initOff + 26, (byte) 0);
            long next1 = insertStringToOffHeap(value.getSecondType().f0, initOff + 27);
            long next2 = insertStringToOffHeap(value.getSecondType().f1, next1);
            long next3 = insertStringToOffHeap(value.getSecondType().f2, next2);
            long next4 = insertStringToOffHeap(value.getSecondType().f3, next3);
            return (int) (next4 - initOff);
        }
    }

    /**
     * 将一个字符串写入到堆外内存
     * @param s 要写入的字符串
     * @param initOff 初始写入偏移
     * @return 下一个要写入的偏移
     */
    private long insertStringToOffHeap(String s, long initOff) {
        byte[] bytes = s.getBytes();
        unsafe.putInt(initOff, bytes.length);
        for (int i = 0; i < bytes.length; i++) {
            unsafe.putByte(initOff + 4 + i, bytes[i]);
        }
        return initOff + 4 + bytes.length;
    }

    /**
     * 从指定的初始偏移位置开始获取一个指定长度的字符串
     * @param initOff 初始偏移位置
     * @param length 要获取的字符串长度
     * @return 获取的字符串
     */
    private String getStringFromOffHeap(long initOff, int length){
        for (int i = 0; i < length; i++) {
            cache[i] = unsafe.getByte(initOff + i);
        }
        return new String(cache, 0, length);
    }


    @Override
    public CommonJoinUnionType<Tuple4<String, String, String, String>, Tuple4<String, String, String, String>> deserializeFromOffHeap(long initOff, long length) {

        CommonJoinUnionType<Tuple4<String, String, String, String>, Tuple4<String, String, String, String>> tuple = new CommonJoinUnionType<>();

        tuple.setNumPartition(unsafe.getInt(initOff));
        tuple.setSelfTimestamp(unsafe.getLong(initOff + 4));
        if (unsafe.getByte(initOff + 12) == 1) {
            tuple.setStoreMode(true);
        } else {
            tuple.setStoreMode(false);
        }

        if (unsafe.getByte(initOff + 13) == 1) {
            tuple.setJoinMode(true);
        } else {
            tuple.setJoinMode(false);
        }

        tuple.setSerialNumOfCoModel(unsafe.getInt(initOff + 14));
        tuple.setOtherTimestamp(unsafe.getLong(initOff + 18));

        // 分R和S两种类型分别读取数据
        boolean isOne;
        if (unsafe.getByte(initOff + 26) == 1) {
            isOne = true;
        } else {
            isOne = false;
        }

        long nextOff = initOff + 27;
        int next1 = unsafe.getInt(nextOff);
        nextOff += 4;
        String s1 = getStringFromOffHeap(nextOff, next1);
        nextOff += next1;

        int next2 = unsafe.getInt(nextOff);
        nextOff += 4;
        String s2 = getStringFromOffHeap(nextOff, next2);
        nextOff += next2;

        int next3 = unsafe.getInt(nextOff);
        nextOff += 4;
        String s3 = getStringFromOffHeap(nextOff, next3);
        nextOff += next3;

        int next4 = unsafe.getInt(nextOff);
        nextOff += 4;
        String s4 = getStringFromOffHeap(nextOff, next4);
        nextOff += next4;

        if (isOne) {
            tuple.one(new Tuple4<>(s1, s2, s3, s4));
        } else {
            tuple.two(new Tuple4<>(s1, s2, s3, s4));
        }

        return tuple;
    }

    @Override
    public long getTimestampInSpecificPosition(long tupleOff) {
        return unsafe.getLong(tupleOff + 4);
    }
}
