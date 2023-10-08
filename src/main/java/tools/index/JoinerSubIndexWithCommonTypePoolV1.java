package tools.index;

import base.CommonJoinUnionType;
import org.apache.flink.core.memory.MemoryUtils;
import sun.misc.Unsafe;

/**
 * 内部采用Unsafe方法减少垃圾回收的子索引结构
 */
public class JoinerSubIndexWithCommonTypePoolV1<F, S, K extends Comparable<K>> extends JoinerSubIndexWithCommonTypePool<F, S, K>{

    // 操纵内存的工具
    private Unsafe unsafe;

    public JoinerSubIndexWithCommonTypePoolV1(int maxSize) {
        super(maxSize);
        unsafe = MemoryUtils.UNSAFE;
    }

    @Override
    protected int simpleInsertOneTupleToLocalIndex(CommonJoinUnionType<F, S> value) {
        CommonJoinUnionType originTuple = localSubIndexArray[currentInsertPosition];
        try {
            long firstType = unsafe.objectFieldOffset(originTuple.getClass().getDeclaredField("firstType"));
            long secondType = unsafe.objectFieldOffset(originTuple.getClass().getDeclaredField("secondType"));
            long numPartition = unsafe.objectFieldOffset(originTuple.getClass().getDeclaredField("numPartition"));
            long selfTimestamp = unsafe.objectFieldOffset(originTuple.getClass().getDeclaredField("selfTimestamp"));
            long storeMode = unsafe.objectFieldOffset(originTuple.getClass().getDeclaredField("storeMode"));
            long joinMode = unsafe.objectFieldOffset(originTuple.getClass().getDeclaredField("joinMode"));
            long serialNumOfCoModel = unsafe.objectFieldOffset(originTuple.getClass().getDeclaredField("serialNumOfCoModel"));
            long otherTimestamp = unsafe.objectFieldOffset(originTuple.getClass().getDeclaredField("otherTimestamp"));

            unsafe.putObject(originTuple, firstType, value.getFirstType());
            unsafe.putObject(originTuple, secondType, value.getSecondType());
            unsafe.putInt(originTuple, numPartition, value.getNumPartition());
            unsafe.putLong(originTuple, selfTimestamp, value.getSelfTimestamp());
            unsafe.putBoolean(originTuple, storeMode, value.isStoreMode());
            unsafe.putBoolean(originTuple, joinMode, value.isJoinMode());
            unsafe.putInt(originTuple, serialNumOfCoModel, value.getSerialNumOfCoModel());
            unsafe.putLong(originTuple, otherTimestamp, value.getOtherTimestamp());

        } catch (NoSuchFieldException e) {
            e.printStackTrace();
            System.err.println("Unsafe替换原来位置的对象失败！");
        }
        currentInsertPosition++;
        return currentInsertPosition - 1;

    }
}
