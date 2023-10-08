package base;

import common.GeneralParameters;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.util.Collector;
import org.apache.log4j.Logger;
import org.apache.zookeeper.ZooKeeper;

import java.util.*;

public class SimpleJoiner<F, S> extends CommonJoiner<F, S> {

    // 用于存储R与S流中的元组的树（Java自带红黑树）
    TreeMap<Double, List<CommonJoinUnionType<F, S>>> R_Tree;
    TreeMap<Double, List<CommonJoinUnionType<F, S>>> S_Tree;

    //Joiner中的zookeeper
    private ZooKeeper zkClient;

    //当前子任务的编号
    private int subTaskIdx;

    //用于保存当前的水印
    private long currentWatermark = 0L;
    //保存上一次执行过期的时间，用于周期性地执行过期
    private long lastExpireTime = 0L;

    //获取logger对象用于日志记录
    private static final Logger logger = Logger.getLogger(SimpleJoiner.class.getName());

    /**
     * 有参构造器，进行范围连接的相关初始化参数设置
     */
    public SimpleJoiner(Time r_TimeWindows, Time s_TimeWindows, KeySelector<F, Double> keySelector_R, KeySelector<S, Double> keySelector_S, double r_surpass_S, double r_behind_S) {
        super(r_TimeWindows, s_TimeWindows, keySelector_R, keySelector_S, r_surpass_S, r_behind_S);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        //获取当前节点编号
        subTaskIdx = getRuntimeContext().getIndexOfThisSubtask();
        //初始化两棵树
        R_Tree = new TreeMap<>();
        S_Tree = new TreeMap<>();
        //初始化上一次过期的时间
        lastExpireTime = 0L;

        logger.info("SimpleJoiner-" + subTaskIdx + ".启动成功！");
    }

    @Override
    public void processElement(CommonJoinUnionType<F, S> value, Context ctx, Collector<CommonJoinUnionType<F, S>> out) throws Exception {
        //更新当前水位线
        currentWatermark = ctx.timerService().currentWatermark();
        //如果事件时间达到过期周期，则进行过期
        if ((currentWatermark - lastExpireTime) >= GeneralParameters.JOINER_EXPIRATION_PERIOD) {
            lastExpireTime = currentWatermark;
            expire(currentWatermark);
        }

        //如果被标记为存储元组，则执行存储操作
        if (value.isStoreMode()) {
            storeTuple(value);
        }

        //如果被标记位连接元组，则执行连接操作
        if (value.isJoinMode()) {
            joinTuples(value, out);
        }

    }

    /**
     * 执行过期操作
     * @param currentWatermark 当前水位线，根据该值和窗口大小计算应该被过期的时间范围
     */
    private void expire(long currentWatermark) {
        logger.info(">>>Joiner-" + subTaskIdx + "-开始执行过期操作，当前时间为：" + currentWatermark);
        //如果对应的R树不为空才开始执行过期
        if (R_Tree.size() != 0) {
            logger.info(">>>Joiner-" + subTaskIdx + ":R树过期前size为-" + R_Tree.size());
            //计算要保留的最小时间戳
            long minR_Timestamp = currentWatermark - R_TimeWindows.toMilliseconds();
            //遍历，删除小于最小时间戳的项
            removeExpireTuplesOfTree(R_Tree, minR_Timestamp);

            logger.info(">>>Joiner-" + subTaskIdx + ":R树过期后size为-" + R_Tree.size());
        }

        //如果对应的S树不为空才开始执行过期
        if (S_Tree.size() != 0) {
            logger.info(">>>Joiner-" + subTaskIdx + ":S树过期前size为-" + S_Tree.size());
            //计算要保留的最小时间戳
            long minS_Timestamp = currentWatermark - S_TimeWindows.toMilliseconds();
            //遍历，删除小于最小时间戳的项
            removeExpireTuplesOfTree(S_Tree, minS_Timestamp);

            logger.info(">>>Joiner-" + subTaskIdx + ":S树过期后size为-" + S_Tree.size());
        }

        logger.info(">>>Joiner-" + subTaskIdx + "-过期操作结束");
    }

    /**
     * 删除制定树中所有时间戳小于给定时间的元组
     * @param storeTree 要删除元组的树
     * @param minTimestamp 小于该时间戳的元组均会被删除
     */
    private void removeExpireTuplesOfTree(TreeMap<Double, List<CommonJoinUnionType<F, S>>> storeTree, long minTimestamp) {
        //使用迭代器删除，防止出现并发修改错误
        Iterator<Map.Entry<Double, List<CommonJoinUnionType<F, S>>>> iterator = storeTree.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<Double, List<CommonJoinUnionType<F, S>>> next = iterator.next();
            List<CommonJoinUnionType<F, S>> value = next.getValue();
            value.removeIf(listNext -> listNext.getSelfTimestamp() < minTimestamp);
            if (value.isEmpty()) {
                iterator.remove();
            }
        }
    }

    /**
     * 将元组存储在对应的树中,其中会根据元组所属流的不同自动存储到不同的树中
     * @param value 要存储的元组
     */
    private void storeTuple(CommonJoinUnionType<F, S> value) throws Exception {
        //根据元组所属流的不同存储于不同的树
        if (value.isOne()) { //R流
            Double key = keySelector_R.getKey(value.getFirstType());
            List<CommonJoinUnionType<F, S>> storedList = R_Tree.get(key);
            if (storedList == null) {  // 如果尚未存储该键值，则新建一项后插入
                List<CommonJoinUnionType<F, S>> newStoredList = new LinkedList<>();
                newStoredList.add(value);
                R_Tree.put(key, newStoredList);
            } else {  // 若已存在，则直接插入
                storedList.add(value);
            }
        } else { //S流
            Double key = keySelector_S.getKey(value.getSecondType());
            List<CommonJoinUnionType<F, S>> storedList = S_Tree.get(key);
            if (storedList == null) {
                List<CommonJoinUnionType<F, S>> newStoredList = new LinkedList<>();
                newStoredList.add(value);
                S_Tree.put(key, newStoredList);
            } else {
                storedList.add(value);
            }
        }
    }

    /**
     * 将元组与对应的元组进行匹配并输出,该方法中会自动判断元组所属的数据流，之后与对应的数据流进行连接
     * @param value 要执行连接的元组
     * @param out 用于输出的上下文
     */
    private void joinTuples(CommonJoinUnionType<F, S> value, Collector<CommonJoinUnionType<F, S>> out) throws Exception {
        if (value.isOne()) {  // 如果为R流元组，则需要与S的树进行连接
            //获取当前元组键值以及要连接的最小时间戳
            Double key = keySelector_R.getKey(value.getFirstType());
            long min_S_Timestamp = value.getSelfTimestamp() - S_TimeWindows.toMilliseconds();
            //用于范围连接，即S - R_behind_S < R < S + R_surpass_S ;R - R_surpass_S < S < R + R_behind_S
            Double S_minKey = key - R_surpass_S;
            Double S_maxKey = key + R_behind_S;

            if (S_minKey.equals(S_maxKey)) {  // 等值连接的情况，单独拿出
                List<CommonJoinUnionType<F, S>> S_MatchedList = S_Tree.get(key);
                if (S_MatchedList == null) {
                    return;
                }
                for (CommonJoinUnionType<F, S> storedSTuple : S_MatchedList) {
                    if (storedSTuple.getSelfTimestamp() < min_S_Timestamp) {
                        continue;
                    }
                    out.collect(value.union(storedSTuple));
                }
            } else {  // 范围连接的情况（不包括右边界）
                SortedMap<Double, List<CommonJoinUnionType<F, S>>> result_S_Map = S_Tree.subMap(S_minKey, S_maxKey);
                Set<Map.Entry<Double, List<CommonJoinUnionType<F, S>>>> entries = result_S_Map.entrySet();
                //遍历满足键值范围的所有列表
                for (Map.Entry<Double, List<CommonJoinUnionType<F, S>>> e : entries) {
                    List<CommonJoinUnionType<F, S>> storedTuplesList = e.getValue();
                    for (CommonJoinUnionType<F, S> tuple : storedTuplesList) {
                        if (tuple.getSelfTimestamp() < min_S_Timestamp) {  // 去除不满足时间的元组
                            continue;
                        }
                        out.collect(value.union(tuple));  // 将输入与遍历到的元组连接，之后输出
                    }
                }// end for
            }

        } else {  // 如果为S流元组,需要与R的元组进行连接
            Double S_Key = keySelector_S.getKey(value.getSecondType());
            long min_R_Timestamp = value.getSelfTimestamp() - R_TimeWindows.toMilliseconds();
            //用于范围连接，即S - R_behind_S < R < S + R_surpass_S ;R - R_surpass_S < S < R + R_behind_S
            Double R_minKey = S_Key - R_behind_S;
            Double R_maxKey = S_Key + R_surpass_S;

            if (R_minKey.equals(R_maxKey)) {  // 等值连接
                List<CommonJoinUnionType<F, S>> R_MatchedList = R_Tree.get(S_Key);
                if (R_MatchedList == null) {
                    return;
                }
                for (CommonJoinUnionType<F, S> storedRTuple : R_MatchedList) {
                    if (storedRTuple.getSelfTimestamp() < min_R_Timestamp) {
                        continue;
                    }
                    out.collect(value.union(storedRTuple));
                }
            } else {  // 范围连接（不包括右边界）
                SortedMap<Double, List<CommonJoinUnionType<F, S>>> result_R_Map = R_Tree.subMap(R_minKey, R_maxKey);
                Set<Map.Entry<Double, List<CommonJoinUnionType<F, S>>>> entries = result_R_Map.entrySet();
                //遍历满足键值范围的所有列表
                for (Map.Entry<Double, List<CommonJoinUnionType<F, S>>> e : entries) {
                    List<CommonJoinUnionType<F, S>> storedRTuplesList = e.getValue();
                    for (CommonJoinUnionType<F, S> tuple : storedRTuplesList) {
                        if (tuple.getSelfTimestamp() < min_R_Timestamp) {
                            continue;
                        }
                        out.collect(value.union(tuple));
                    }
                }// end for

            }
        }// s流处理结束
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {

    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {

    }

}
