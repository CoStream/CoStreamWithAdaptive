package base;

import common.GeneralParameters;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.util.Collector;
import org.apache.log4j.Logger;
import org.apache.zookeeper.ZooKeeper;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.TreeMap;

/**
 * 采用链式子窗口索引的Joiner，其中每个子窗口中元组的数量是固定的，主要用于比较BiModel，每个子索引中均是TreeMap类型
 * 只能处理等值连接
 */
public class SimpleJoinerWithConstraintNumberSubWindow<F, S> extends CommonJoiner<F, S> {

    //采用子索引的方式存储元组，格式为-- 最大时间戳，最小时间戳，索引, 子索引元组数量
    List<Tuple4<Long, Long, TreeMap<Double, List<CommonJoinUnionType<F, S>>>, Long>> R_Tree_set;
    List<Tuple4<Long, Long, TreeMap<Double, List<CommonJoinUnionType<F, S>>>, Long>> S_Tree_set;

    //每个子树的元组数量（固定值）
    private long subTreeNum = GeneralParameters.CONSTRAINT_SUB_INDEX_NUMBER;

    //Joiner中的zookeeper
    private ZooKeeper zkClient;

    //当前子任务的编号
    private int subTaskIdx;

    //用于保存当前的水印
    private long currentWatermark = 0L;
    //保存上一次执行过期的时间，用于周期性地执行过期
    private long lastExpireTime = 0L;

    //获取logger对象用于日志记录
    private static final Logger logger = Logger.getLogger(SimpleJoinerWithConstraintNumberSubWindow.class.getName());

    /**
     * 有参构造器，进行范围连接的相关初始化参数设置
     */
    public SimpleJoinerWithConstraintNumberSubWindow(Time r_TimeWindows, Time s_TimeWindows, KeySelector<F, Double> keySelector_R, KeySelector<S, Double> keySelector_S, double r_surpass_S, double r_behind_S) {
        super(r_TimeWindows, s_TimeWindows, keySelector_R, keySelector_S, r_surpass_S, r_behind_S);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        //获取当前节点编号
        subTaskIdx = getRuntimeContext().getIndexOfThisSubtask();
        //初始化两棵树集合
        R_Tree_set = new LinkedList<>();
        R_Tree_set.add(new Tuple4<>(Long.MIN_VALUE, Long.MAX_VALUE, new TreeMap<>(), 0L));
        S_Tree_set = new LinkedList<>();
        S_Tree_set.add(new Tuple4<>(Long.MIN_VALUE, Long.MAX_VALUE, new TreeMap<>(), 0L));
        //初始化上一次过期的时间
        lastExpireTime = 0L;

        logger.info(this.getClass().getName() + "-" + subTaskIdx + ".启动成功！");
        logger.info("每个子树的最大元组数量为-" + subTreeNum);
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
        if (R_Tree_set.size() > 1) {
            logger.info(">>>Joiner-" + subTaskIdx + ":R树过期前size为-" + R_Tree_set.size() + ";位置0的子索引的总元组数量为：" + (R_Tree_set.get(0).f3));
            //计算要保留的最小时间戳
            long minR_Timestamp = currentWatermark - R_TimeWindows.toMilliseconds();
            //遍历，删除小于最小时间戳的项
            removeExpireTuplesOfTree(R_Tree_set, minR_Timestamp);

            logger.info(">>>Joiner-" + subTaskIdx + ":R树过期后size为-" + R_Tree_set.size() + ";位置0的子索引的总元组数量为：" + (R_Tree_set.get(0).f3));
        }

        //如果对应的S树不为空才开始执行过期
        if (S_Tree_set.size() > 1) {
            logger.info(">>>Joiner-" + subTaskIdx + ":S树过期前子索引数为-" + S_Tree_set.size() + ";位置0的子索引的总元组数量为：" + (S_Tree_set.get(0).f3));
            //计算要保留的最小时间戳
            long minS_Timestamp = currentWatermark - S_TimeWindows.toMilliseconds();
            //遍历，删除小于最小时间戳的项
            removeExpireTuplesOfTree(S_Tree_set, minS_Timestamp);

            logger.info(">>>Joiner-" + subTaskIdx + ":S树过期后子索引数为-" + S_Tree_set.size() + ";位置0的子索引的总元组数量为：" + (S_Tree_set.get(0).f3));
        }

        logger.info(">>>Joiner-" + subTaskIdx + "-过期操作结束");
    }

    /**
     * 删除制定树中所有时间戳小于给定时间的元组
     * @param storeTreeSet 要删除元组的树集合
     * @param minTimestamp 小于该时间戳的元组均会被删除
     */
    private void removeExpireTuplesOfTree(
            List<Tuple4<Long, Long, TreeMap<Double, List<CommonJoinUnionType<F, S>>>, Long>> storeTreeSet,
            long minTimestamp) {
        //使用迭代器删除，防止出现并发修改错误
        Iterator<Tuple4<Long, Long, TreeMap<Double, List<CommonJoinUnionType<F, S>>>, Long>> iterator = storeTreeSet.iterator();
        while (iterator.hasNext()) {
            Tuple4<Long, Long, TreeMap<Double, List<CommonJoinUnionType<F, S>>>, Long> next = iterator.next();
            if (next.f0 < minTimestamp) {
                next.f2.clear();
                iterator.remove();
            }
        }

    }

    /**
     * 存储一个到达的元组
     * @param value 要存储的元组
     */
    private void storeTuple(CommonJoinUnionType<F, S> value) throws Exception {
        if (value.isOne()) {
            Tuple4<Long, Long, TreeMap<Double, List<CommonJoinUnionType<F, S>>>, Long> storeRTree = R_Tree_set.get(0);
            if (value.getSelfTimestamp() > storeRTree.f0) {
                storeRTree.f0 = value.getSelfTimestamp();
            }
            if (value.getSelfTimestamp() < storeRTree.f1) {
                storeRTree.f1 = value.getSelfTimestamp();
            }

            storeTupleToSubTree(value, storeRTree.f2);

            storeRTree.f3 += 1;
            if (storeRTree.f3 >= subTreeNum) {
                R_Tree_set.add(0, new Tuple4<>(Long.MIN_VALUE, Long.MAX_VALUE, new TreeMap<>(), 0L));
                logger.info("R树新建子树，触发新建子树的元组的时间戳为：" + value.getSelfTimestamp() + ";上一个活动子树元组数量为：" + storeRTree.f3);
            }

        } else {
            Tuple4<Long, Long, TreeMap<Double, List<CommonJoinUnionType<F, S>>>, Long> storeSTree = S_Tree_set.get(0);
            if (value.getSelfTimestamp() > storeSTree.f0) {
                storeSTree.f0 = value.getSelfTimestamp();
            }
            if (value.getSelfTimestamp() < storeSTree.f1) {
                storeSTree.f1 = value.getSelfTimestamp();
            }

            storeTupleToSubTree(value, storeSTree.f2);

            storeSTree.f3 += 1;
            if (storeSTree.f3 >= subTreeNum) {
                S_Tree_set.add(0, new Tuple4<>(Long.MIN_VALUE, Long.MAX_VALUE, new TreeMap<>(), 0L));
                logger.info("S树新建子树，触发新建子树的元组的时间戳为：" + value.getSelfTimestamp() + ";上一个活动子树元组数量为：" + storeSTree.f3);
            }
        }
    }

    /**
     * 将元组存储在对应的树中
     * @param storeTree 该元组要被存储的树
     * @param value 要存储的元组
     */
    private void storeTupleToSubTree(CommonJoinUnionType<F, S> value, TreeMap<Double, List<CommonJoinUnionType<F, S>>> storeTree) throws Exception {
        //根据元组所属流的不同存储于不同的树
        Double key = 0.0;
        if (value.isOne()) { //R流
            key = keySelector_R.getKey(value.getFirstType());
        }else {
            key = keySelector_S.getKey(value.getSecondType());
        }
        List<CommonJoinUnionType<F, S>> storedList = storeTree.get(key);
        if (storedList == null) {  // 如果尚未存储该键值，则新建一项后插入
            List<CommonJoinUnionType<F, S>> newStoredList = new LinkedList<>();
            newStoredList.add(value);
            storeTree.put(key, newStoredList);
        } else {  // 若已存在，则直接插入
            storedList.add(value);
        }

    }

    /**
     * 连接一个到达的元组，与所有子树
     * @param value 要进行连接的元组
     */
    private void joinTuples(CommonJoinUnionType<F, S> value, Collector<CommonJoinUnionType<F, S>> out) throws Exception {
        if (value.isOne()) {  //R流元组
            List<CommonJoinUnionType<F, S>> allResultList = new LinkedList<>();
            long min_S_Timestamp = value.getSelfTimestamp() - S_TimeWindows.toMilliseconds();
            for (Tuple4<Long, Long, TreeMap<Double, List<CommonJoinUnionType<F, S>>>, Long> joinTree : S_Tree_set) {
                if (joinTree.f0 < min_S_Timestamp) {
                    continue;
                }
                List<CommonJoinUnionType<F, S>> resultList = joinTuplesWithSubTree(value, joinTree.f2);
                if (resultList != null) {
                    allResultList.addAll(resultList);
                }
            }
            for (CommonJoinUnionType<F, S> t : allResultList) {
                out.collect(value.union(t));
            }
        } else {  //S流元组
            List<CommonJoinUnionType<F, S>> allResultList = new LinkedList<>();
            long min_R_Timestamp = value.getSelfTimestamp() - R_TimeWindows.toMilliseconds();
            for (Tuple4<Long, Long, TreeMap<Double, List<CommonJoinUnionType<F, S>>>, Long> joinTree : R_Tree_set) {
                if (joinTree.f0 < min_R_Timestamp) {
                    continue;
                }
                List<CommonJoinUnionType<F, S>> resultList = joinTuplesWithSubTree(value, joinTree.f2);
                if (resultList != null) {
                    allResultList.addAll(resultList);
                }
            }
            for (CommonJoinUnionType<F, S> t : allResultList) {
                out.collect(value.union(t));
            }
        }
    }

    /**
     * 将元组与对应的元组进行匹配并输出,该方法中会自动判断元组所属的数据流，之后与对应的数据流进行连接(等值)
     */
    private List<CommonJoinUnionType<F, S>> joinTuplesWithSubTree(
            CommonJoinUnionType<F, S> value,
            TreeMap<Double, List<CommonJoinUnionType<F, S>>> joinTree
    ) throws Exception {

        LinkedList<CommonJoinUnionType<F, S>> resultList = new LinkedList<>();

        if (value.isOne()) {  // 如果为R流元组，则需要与S的树进行连接
            //获取当前元组键值以及要连接的最小时间戳
            Double key = keySelector_R.getKey(value.getFirstType());
            long min_S_Timestamp = value.getSelfTimestamp() - S_TimeWindows.toMilliseconds();
            List<CommonJoinUnionType<F, S>> S_MatchedList = joinTree.get(key);
            if (S_MatchedList == null) {
                return null;
            }
            for (CommonJoinUnionType<F, S> storedSTuple : S_MatchedList) {
                if (storedSTuple.getSelfTimestamp() < min_S_Timestamp) {
                    continue;
                }
                resultList.add(storedSTuple);
            }
        } else {  // 如果为S流元组,需要与R的元组进行连接
            Double S_Key = keySelector_S.getKey(value.getSecondType());
            long min_R_Timestamp = value.getSelfTimestamp() - R_TimeWindows.toMilliseconds();
            List<CommonJoinUnionType<F, S>> R_MatchedList = joinTree.get(S_Key);
            if (R_MatchedList == null) {
                return null;
            }
            for (CommonJoinUnionType<F, S> storedRTuple : R_MatchedList) {
                if (storedRTuple.getSelfTimestamp() < min_R_Timestamp) {
                    continue;
                }
                resultList.add(storedRTuple);
            }

        }// s流处理结束
        return resultList;
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {

    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {

    }

}
