package coModel.replaceable;

import base.CommonJoinUnionType;
import coModel.tools.sync.ZookeeperNodePathSetForCoModel;
import coModel.tools.tune.MyTimeRecorder;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import tools.index.CommonJoinerSubIndex;
import tools.index.JoinerSubIndexWithTreeMap;

/**
 * CoModel的Joiner，其中采用链式索引方法，且子索引是基于TreeMap实现的，
 * 并且在该类中会记录存储和连接元组的执行时间并上传
 * @param <F> 第一个流的类型
 * @param <S> 第二个流的类型
 */
public class CoModelJoiner_WithLinkedTreeMapIndex<F, S> extends CommonLinkedIndexJoiner<F, S> {

    //用于记录存储元组和连接元组执行时间的记录器
    private MyTimeRecorder storeProcessTimeRecorder = new MyTimeRecorder(1);
    private MyTimeRecorder joinProcessTimeRecorder = new MyTimeRecorder(1);
    //用于保证每隔一定时间才会记录一次存储元组和连接元组执行时间的定时器
    private final MyTimer processTimeRecordTimer = new MyTimer();
    //用于标志当前是否需要更新元组处理时间的标志
    private boolean isStoreProcessTimeShouldUpdate = false;
    private boolean isJoinProcessTimeShouldUpdate = false;

    //指示Joiner中每隔多长时间更新一次存储元组和连接元组的处理时间(单位：ms)
    private static final long PROCESS_TIME_UPDATE_INTERVAL = 1000;

    //用于进行Metric指标上传时的值
    private long storeTupleProcessTime = 44;  //存储元组的处理时间（单位：纳秒）
    private long joinTupleProcessTime = 44;  //连接元组的处理时间（单位：纳秒）

    /**
     * 有参构造器，进行范围连接的相关初始化参数设置
     */
    public CoModelJoiner_WithLinkedTreeMapIndex(Time r_TimeWindows, Time s_TimeWindows, KeySelector<F, Double> keySelector_R, KeySelector<S, Double> keySelector_S, double r_surpass_S, double r_behind_S) {
        super(r_TimeWindows, s_TimeWindows, keySelector_R, keySelector_S, r_surpass_S, r_behind_S);
    }

    /**
     * 初始化用于设置指标的定时器，该定时器每隔指定的时间指示上传一次存储元组和连接元组的处理时间,
     * 在该方法中，每隔指定的时间便会将是否需要更新处理时间的标记置为true、
     * 此外，在该方法中还会初始化一些与处理时间上传zookeeper相关的节点，目的是防止读取对应节点时报错
     */
    private void initProcessTimeRecordTimerAndInitCoordinateZookeeperNode() {
        //设置定时器
        processTimeRecordTimer.schedule(new MyTimerTask() {
            @Override
            public void run() {
                isStoreProcessTimeShouldUpdate = true;
                isJoinProcessTimeShouldUpdate = true;
            }
        }, 10000, PROCESS_TIME_UPDATE_INTERVAL);

        logger.info("CoModel-Joiner-" + subTaskIdx + ":处理时间上传指标时间定时器设置成功！");

        //初始化相应的zookeeper节点
        synchronizer.setZookeeperNodeContent(
                ZookeeperNodePathSetForCoModel.JOINER_UPLOAD_LOCAL_STORE_TUPLE_PROCESS_TIME_PREFIX + subTaskIdx,
                ("4" + storeTupleProcessTime).getBytes());
        synchronizer.setZookeeperNodeContent(
                ZookeeperNodePathSetForCoModel.JOINER_UPLOAD_LOCAL_JOIN_TUPLE_PROCESS_TIME_PREFIX + subTaskIdx,
                ("4" + joinTupleProcessTime).getBytes());

    }


    @Override
    protected CommonJoinerSubIndex<Double, CommonJoinUnionType<F, S>> creatOneNewSubIndex() {
        logger.info("CoModel-Joiner-创建了一个新的子索引，子索引的类型是TreeMap。");
        return new JoinerSubIndexWithTreeMap<Double, CommonJoinUnionType<F, S>>();
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        //初始化用于指示上传存储元组和连接元组处理时间的定时器,以及初始化对应的Zookeeper节点内容
        initProcessTimeRecordTimerAndInitCoordinateZookeeperNode();

        logger.info("当前启动的是内部子索引结构为Tree Map的CoModel的Joiner！");

    }

    @Override
    public void close() throws Exception {
        super.close();
        processTimeRecordTimer.cancel();
    }

    /**
     * 处理正常的元组，包括元组的插入，探测，过期以及输出等操作
     * 该类的该方法加入了记录存储元组和连接元组的处理时间的功能
     */
    @Override
    protected void processNormalInputTuple(CommonJoinUnionType<F, S> value, Context ctx, Collector<CommonJoinUnionType<F, S>> out) throws Exception {

        //==============================================================================================================
        //=-------------------------------------------------元组存储--------------------------------------------------===
        //==============================================================================================================
        if (value.isStoreMode()) {

            //开始记录时间
            if (isStoreProcessTimeShouldUpdate) {
                storeProcessTimeRecorder.startRecord();
            }

            //---------------------------------------------处理存储元组--------------------------------------------------
            insertTuple(value);
            //----------------------------------------------------------------------------------------------------------

            //结束记录时间,并上传
            if (isStoreProcessTimeShouldUpdate) {
                storeTupleProcessTime = storeProcessTimeRecorder.stopRecordAndReturn();
                synchronizer.setZookeeperNodeContent(
                        ZookeeperNodePathSetForCoModel.JOINER_UPLOAD_LOCAL_STORE_TUPLE_PROCESS_TIME_PREFIX + subTaskIdx,
                        ("" + storeTupleProcessTime).getBytes());
                isStoreProcessTimeShouldUpdate = false;
            }

        }

        //==============================================================================================================
        //=----------------------------------------------元组探测及输出-----------------------------------------------===
        //==============================================================================================================
        if (value.isJoinMode()) {

            //开始记录时间
            if (isJoinProcessTimeShouldUpdate) {
                joinProcessTimeRecorder.startRecord();
            }

            //----------------------------------------------处理连接元组------------------------------------------------
            probeTupleAndOutput(value, ctx, out);
            //---------------------------------------------------------------------------------------------------------

            //结束记录时间,并上传
            if (isJoinProcessTimeShouldUpdate) {
                joinTupleProcessTime = joinProcessTimeRecorder.stopRecordAndReturn();
                synchronizer.setZookeeperNodeContent(
                        ZookeeperNodePathSetForCoModel.JOINER_UPLOAD_LOCAL_JOIN_TUPLE_PROCESS_TIME_PREFIX + subTaskIdx,
                        ("" + joinTupleProcessTime).getBytes());
                isJoinProcessTimeShouldUpdate = false;
            }

        }

        //==============================================================================================================
        //=-------------------------------------------------元组过期--------------------------------------------------===
        //==============================================================================================================
        currentWatermark = ctx.timerService().currentWatermark();
        if ((currentWatermark - lastExpireWatermark) > expirePeriod) {  //如果两次过期之间的间隔到达阈值，则执行过期
            lastExpireWatermark = currentWatermark;
            expireOperation(currentWatermark);
        }

    }
}
