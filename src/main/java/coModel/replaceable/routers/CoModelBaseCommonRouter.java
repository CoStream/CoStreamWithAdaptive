package coModel.replaceable.routers;

import base.CommonJoinUnionType;
import base.CommonRouter;
import coModel.CoModelCoordinator;
import coModel.CoModelParameters;
import coModel.tools.SignalMessageFactoryForCoModel;
import coModel.tools.sync.RichListenerWithoutReturn;
import coModel.tools.sync.TransferProtocolBasedZookeeper;
import coModel.tools.sync.ZookeeperBasedSynchronizer;
import coModel.tools.sync.ZookeeperNodePathSetForCoModel;
import common.GeneralParameters;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.util.Collector;
import org.apache.log4j.Logger;
import org.apache.zookeeper.WatchedEvent;
import tools.common.ParseExperimentParametersTool;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 基础班的CoModel的Router，带有基本的CoModel 的 Router 功能，可以直接使用，其中的参数是通过Zookeeper获取的
 * 该方法和CoModelRouter一开始是一样的（在2023年6月25日），只是作为备份被其他种类的Router继承
 * @param <F> 第一个流的类型
 * @param <S> 第二个流的类型
 */
public class CoModelBaseCommonRouter<F, S> extends CommonRouter<F, S> {

    //系统中总的Router数量与Joiner数量
    protected int totalRouterNum;
    protected int totalJoinerNum;

    //系统中用于存储R与S的Joiner数量
    protected int R_totalStoreNum;
    protected int S_totalStoreNum;
    //系统中在R（或S）的所有存储节点中，每个R（或S）元组存储的副本数量
    protected int R_copiesNum;
    protected int S_copiesNum;

    //R中元组与S中存储元组的序列号（用于指导存储流分区，序列号最大数量为-对应-流的存储节点数量，并且每个元组到达时序列号依次递增）
    protected int currentSerialNumOf_Store_R = 0;
    protected int currentSerialNumOf_Store_S = 0;
    //R中元组与S中连接元组的序列号（用于指导连接流分区，序列号最大数量为-相对-流的存储节点数量，并且每个元组到达时序列号依次递增）
    protected int currentSerialNumOf_Join_R = 0;
    protected int currentSerialNumOf_Join_S = 0;

    //用于存储从协调器接收的最新的分区方案
    protected Tuple4<Integer, Integer, Integer, Integer> newPartitionScheme;

    //当前Router编号：
    protected int subTaskNum;

    //用于线程同步的锁
    protected final Lock lock = new ReentrantLock();
    //用于唤醒Router的条件
    protected Condition wakeUpRouterCondition = lock.newCondition();

    //Router中的基于Zookeeper实现的同步器，用于与中央协调器以及Joiner进行同步
    protected ZookeeperBasedSynchronizer synchronizer;

    //指标监控
    protected Meter myMeter;

    //当前Router的状态
    protected final RouterStatus routerState = new RouterStatus();

    //用于处理信号消息的对象
    protected final SignalMessageFactoryForCoModel<F, S> signalFactory = new SignalMessageFactoryForCoModel<>();

    //获取logger对象用于日志记录
    protected static final Logger logger = Logger.getLogger(CoModelBaseCommonRouter.class.getName());


    /**
     * 通用的有参构造
     */
    public CoModelBaseCommonRouter(Time r_TimeWindows, Time s_TimeWindows, double r_surpass_S, double r_behind_S, KeySelector<F, Double> keySelector_R, KeySelector<S, Double> keySelector_S) {
        super(r_TimeWindows, s_TimeWindows, r_surpass_S, r_behind_S, keySelector_R, keySelector_S);
    }

    @Override
    public void open(Configuration parameters) throws Exception {

        //初始化基于Zookeeper实现的协调，用于实现远程通讯及同步
        initZookeeper();

        //初始化相关参数
        initParameters();

        //初始化中央协调器，中央协调器在此仅在编号为0的Router中被启动，同时创建判定系统存活的节点
        initCoordinatorInSpecialRouter(0);

        //初始化要用到的监听器
        initRouterListener();

        //初始化指标系统，用于在WebUI中显示系统的监控指标
        initMetricSystem();

        //打印当前系统状态
        logCoModelCurrentStatus(R_totalStoreNum, S_totalStoreNum, R_copiesNum, S_copiesNum);

        logger.info("CoModel-Router-" + subTaskNum + ":系统初始化完成！");

    }

    /**
     * 初始化系统的相关参数，并打印系统的配置信息
     */
    private void initParameters() {

        // 获取保存在Zookeeper中的实验配置
        byte[] argsBytes = synchronizer.getZookeeperNodeContent(ParseExperimentParametersTool.ZOOKEEPER_EXP_CONFIGS_PATH_OF_ALL_MODEL);
        logger.info("CoModel-Router-获取Zookeeper中的实验配置参数成功！");

        totalRouterNum = CoModelParameters.TOTAL_NUM_OF_ROUTER;
        totalJoinerNum = CoModelParameters.TOTAL_NUM_OF_JOINER;
        R_totalStoreNum = CoModelParameters.TOTAL_R_JOINER_NUM;
        S_totalStoreNum = CoModelParameters.TOTAL_S_JOINER_NUM;

        // 复制数采用Zookeeper中的配置
        R_copiesNum = Integer.parseInt(
                ParseExperimentParametersTool
                        .parseExpParametersForNameWithinByteArray(argsBytes, ParseExperimentParametersTool.PARAM_NAME_OF_R_COPY_NUM));//CoModelParameters.INIT_R_NMU_OF_COPIES;
        S_copiesNum = Integer.parseInt(
                ParseExperimentParametersTool
                        .parseExpParametersForNameWithinByteArray(argsBytes, ParseExperimentParametersTool.PARAM_NAME_OF_S_COPY_NUM));//CoModelParameters.INIT_S_NMU_OF_COPIES;

        //获取当前任务的编号
        subTaskNum = getRuntimeContext().getIndexOfThisSubtask();

        //打印系统初始化配置信息
        logger.info("CoModel-Router-" + subTaskNum
                + ":系统初始化配置完成！：总Router数量为：" + totalRouterNum
                + ";总Joiner数量为：" + totalJoinerNum);
    }

    /**
     * 初始化Zookeeper及其相关配置,
     */
    private void initZookeeper() {
        synchronizer = new ZookeeperBasedSynchronizer(GeneralParameters.CONNECT_STRING, GeneralParameters.SESSION_TIMEOUT);
        logger.info("CoModel-Router-" + subTaskNum + ":zookeeper同步器创建成功");
    }

    /**
     * 创建标志系统存活的zookeeper节点，同时在指定的Router实例中创建并启动中央协调器节点
     * --中央协调器应该仅在一个Router中被创建
     * --创建系统存活的节点应在创建协调器之前
     * @param routerNum 要在其中创建协调器以及创建标识系统存活节点的Router编号
     */
    private void initCoordinatorInSpecialRouter(int routerNum) {
        if (subTaskNum == routerNum) {
            logger.info("CoModel-Router-" + subTaskNum + ":准备创建中央协调器以及标志系统存活的节点-" + ZookeeperNodePathSetForCoModel.CO_MODEL_SYSTEM_ALIVE);
            creatSystemZookeeperPrefix();
            synchronizer.creatZookeeperNodeEphemeral(ZookeeperNodePathSetForCoModel.CO_MODEL_SYSTEM_ALIVE, "SystemAlive".getBytes());
            logger.info("CoModel-Router-" + subTaskNum + ":创建标志系统存活的zookeeper节点成功！");
            new CoModelCoordinator().start();
            logger.info("CoModel-Router-" + subTaskNum + ":启动中央协调器成功！");
        }
    }

    /**
     * 创建整个系统所需的所有前缀，均为永久节点
     * 在创建中央协调器的Router中，在正式创建中央协调器前，应该调用该方法，否则标记系统存活的节点无法创建
     */
    private void creatSystemZookeeperPrefix() {
        synchronizer.creatZookeeperNodePersistent(CoModelParameters.ZOOKEEPER_PREFIX_OF_CO_MODEL, "CoModel".getBytes());
        synchronizer.creatZookeeperNodePersistent(CoModelParameters.ZOOKEEPER_PREFIX_OF_CO_MODEL_COORDINATOR, "Coordinator".getBytes());
        synchronizer.creatZookeeperNodePersistent(CoModelParameters.ZOOKEEPER_PREFIX_OF_CO_MODEL_ROUTER, "Router".getBytes());
        synchronizer.creatZookeeperNodePersistent(CoModelParameters.ZOOKEEPER_PREFIX_OF_CO_MODEL_JOINER, "Joiner".getBytes());
        synchronizer.creatZookeeperNodePersistent(CoModelParameters.ZOOKEEPER_PREFIX_OF_CO_MODEL_MONITOR, "Monitor".getBytes());
    }

    /**
     * 初始化本地的一些监听器，用于监听zookeeper中的事件
     */
    private void initRouterListener() {
        //创建Router中用于接收一般性消息的循环监视器
        synchronizer.creatLoopRichListenerWithoutReturn(ZookeeperNodePathSetForCoModel.ROUTER_RECEIVE_COORDINATOR_COMMON_NODE, new RichListenerWithoutReturn() {
            @Override
            public void onTrigger(WatchedEvent event, byte[] data) {
                logger.info("CoModel-Router-" + subTaskNum + ":接收到传来的一般消息，现在开始处理。。。");
                processCommonRouterReceiveMess(event, data);
            }
        });
        logger.info("CoModel-Router-" + subTaskNum + ":监听一般消息的监听器初始化完成！");

        //创建Router中用于监听中央协调器触发Joiner内存变更同步回合的监听器
        synchronizer.listeningRichForOneToAllManySyncRoundOnLoop(ZookeeperNodePathSetForCoModel.C_R_J_CHANGE_JOINER_MEMORY_SYNC_ROUND, new RichListenerWithoutReturn() {
            @Override
            public void onTrigger(WatchedEvent event, byte[] data) {
                TransferProtocolBasedZookeeper receiveMess = new TransferProtocolBasedZookeeper(data);

                if (receiveMess.isChangeCopiesNum()) {
                    //获取新的分区方案
                    newPartitionScheme = receiveMess.getPartitionSchemeOfCoModel();
                    //将Router的状态置为开始进行Joiner对齐，
                    routerState.setStartProcessJoinerAlignment(true);
                    logger.info("CoModel-Router-" + subTaskNum + ":接收到通知Joiner变更内存开始的消息，准备开始对齐！接受到新的分区方案：" + newPartitionScheme);
                } else {
                    logger.info("CoModel-Router-" + subTaskNum + ":接收到未知类型的消息！");
                }
            }
        });
        logger.info("CoModel-Router-" + subTaskNum + ":监听中央协调器触发Joiner内存变更同步回合的监听器初始化完成！");
    }

    /**
     * 初始化Flink的指标系统，用于在WebUI中显示一些指标信息
     */
    private void initMetricSystem() {
        //指标系统初始化
        MetricGroup my_custom_throughput_monitor = getRuntimeContext()
                .getMetricGroup()
                .addGroup("my_custom_throughput_monitor");
        Counter inputCounter = my_custom_throughput_monitor.counter("InputCounter");
        myMeter = my_custom_throughput_monitor.meter("MyThroughputMeter",new MeterView(inputCounter,30));

        logger.info("CoModel-Router-" + subTaskNum + ":指标系统初始化成功！");
    }

    /**
     * 打印系统当前的状态配置信息，即每个流的存储节点数量以及存储副本数量等信息
     * @param totalStoreNumOf_R R的存储节点数量
     * @param totalStoreNumOf_S S的存储节点数量
     * @param currentCopiesNumOf_R R的存储节点中每个R中的元组的存储副本数量
     * @param currentCopiesNumOf_S S的存储节点中每个S中的元组的存储副本数量
     */
    private void logCoModelCurrentStatus(int totalStoreNumOf_R, int totalStoreNumOf_S, int currentCopiesNumOf_R, int currentCopiesNumOf_S) {
        logger.info("CoModel-Router-" + subTaskNum
                + "-当前状态: R的存储节点数量为-" + totalStoreNumOf_R
                + ";R中每个元组的存储元组副本数为-" + currentCopiesNumOf_R
                + ";S的存储节点数量为-" + totalStoreNumOf_S
                + ";S中每个元组的存储元组副本数为-" + currentCopiesNumOf_S
                + "。");
    }

    /**
     * 处理由通用Router接收节点传来的一般性的通知Router的消息，根据消息的类型分情况讨论
     * @param event zookeeper事件
     * @param data 传来的消息内容
     */
    private void  processCommonRouterReceiveMess(WatchedEvent event, byte[] data) {
        TransferProtocolBasedZookeeper receiveMess = new TransferProtocolBasedZookeeper(data);
        //根据消息类型的不同分别加以处理
        //处理接收到的通知Joiner完成同步的消息
        if (receiveMess.isNotifyJoinerSyncCompleteMess()) {
            logger.info("CoModel-Router-" + subTaskNum + ":接收到通知Joiner完成同步的消息，开始处理。。。");
            notifyJoinerSyncCompleteMessHandler(receiveMess);
        }

        logger.info("CoModel-Router-" + subTaskNum + ":一般性通知Router的消息处理完毕！");
    }

    /**
     * 处理Router接收到的通知Joiner完成同步的消息
     * @param receiveMess 接收到的消息
     */
    private void notifyJoinerSyncCompleteMessHandler(TransferProtocolBasedZookeeper receiveMess) {
        lock.lock();
        try {
            //将当前系统状态置为 非进行Joiner内存变更的同步中，同时重置发送对齐消息的状态
            routerState.setStartProcessJoinerAlignment(false);
            routerState.setRouterCompleteSendAlignmentMess(false);

            //用新的分区方案代替本地的分区方案
            R_totalStoreNum = newPartitionScheme.f0;
            R_copiesNum = newPartitionScheme.f1;
            S_totalStoreNum = newPartitionScheme.f2;
            S_copiesNum = newPartitionScheme.f3;

            //唤醒Router主线程
            logger.info("CoModel-Router-" + subTaskNum + ":更改本地分区方案完成！准备唤醒主线程。。。");
            //打印当前系统状态
            logCoModelCurrentStatus(R_totalStoreNum, S_totalStoreNum, R_copiesNum, S_copiesNum);
            wakeUpRouterCondition.signalAll();

        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("CoModel-Router-" + subTaskNum + ":处理接收到的通知Joiner完成同步的消息失败！");
        }finally {
            lock.unlock();
        }
    }

    /**
     * 一般由Router主线程调用，作用是将当前线程阻塞在条件 wakeUpRouterCondition 上
     */
    private void blockRouterMainThread() {
        lock.lock();
        try {
            wakeUpRouterCondition.await();
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("CoModel-Router-" + subTaskNum + ":Router主线程阻塞失败！");
        }finally {
            lock.unlock();
        }
    }

    /**
     * 一般由zookeeper线程调用，作用是唤醒阻塞在 wakeUpRouterCondition 上的 Router主线程
     */
    private void wakeUpRouterMainThread() {
        lock.lock();
        try {
            wakeUpRouterCondition.signalAll();
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("CoModel-Router-" + subTaskNum + ":唤醒Router主线程失败！");
        }finally {
            lock.unlock();
        }
    }

    /**
     * 给定存储节点的数量以及副本的数量，返回所有要发往的存储分区
     * @param totalStoreJoinerNum 对应流的存储节点数量
     * @param currentCopiesNum 对应流中元组的存储副本数量
     * @param tupleSerialNum 元组对应的序列号
     * @return 所有的存储分区编号列表（不带偏移，若要求的是S中元组的存储分区号，则前面需要加上偏移量）
     */
    protected List<Integer> getStorePartitionNumList(int totalStoreJoinerNum, int currentCopiesNum, int tupleSerialNum) {
        //结果列表
        LinkedList<Integer> storePartitionNumList = new LinkedList<>();

        // 当前序列号属于第几组
        int groupNum = tupleSerialNum / currentCopiesNum;

        // 对应组内的所有节点均为该元组的存储分区
        for (int i = groupNum * currentCopiesNum; i < ((groupNum + 1) * currentCopiesNum); i++) {
            storePartitionNumList.add(i);
        }

        return storePartitionNumList;
    }

    /**
     * 给定要连接的流的存储节点的数量以及其副本的数量，返回所有要发往的连接分区
     * @param opTotalStoreJoinerNum 要连接的相对流的存储节点数量
     * @param opCurrentCopiesNum 要连接的相对流中元组的存储副本数量
     * @param tupleSerialNum 本流中元组对应的序列号
     * @return 所有的存储分区编号列表（不带偏移，若要求的是R中元组的连接分区号，则前面需要加上偏移量）
     */
    protected List<Integer> getJoinPartitionNumList(int opTotalStoreJoinerNum, int opCurrentCopiesNum, int tupleSerialNum) {
        LinkedList<Integer> joinPartitionNumList = new LinkedList<>();

        //获取相对流中元组被分为了多少组（每个相对流中的元组的所有副本被存储在一个组内的所有节点中）
        int totalGroupNum = opTotalStoreJoinerNum / opCurrentCopiesNum;

        //获取该元组对应的连接元组在每组中的编号，只需要保证每个组内有一个连接元组即可
        int joinNumInEachGroup = tupleSerialNum % opCurrentCopiesNum;

        //获取所有的连接元组分区，每个组内有一个分区
        for (int i = 0; i < totalGroupNum; i++) {
            joinPartitionNumList.add((i * opCurrentCopiesNum + joinNumInEachGroup));
        }

        return joinPartitionNumList;
    }


    @Override
    public void processElement(CommonJoinUnionType<F, S> value, Context ctx, Collector<CommonJoinUnionType<F, S>> out) throws Exception {
        //设置元组的时间戳，后面的存储元组与连接元组需要以此时间戳为基础进行创建
        value.setSelfTimestamp(ctx.timestamp());
        //指标加1，用于计算平均吞吐
        myMeter.markEvent();

        //根据当前Router的状态执行不同的操作
        if (routerState.isStartProcessJoinerAlignment()) {
            //处理Joiner对齐，向下游发送对齐消息
            processJoinerAlignment(value, ctx, out);
        } else {
            //处理正常的元组
            processNormalTuples(value, ctx, out);
        }

    }

    /**
     * 当Router接收到中央协调器通知的开启Joiner内存变更同步回合时调用该方法，向所有Joiner发送信号消息，让所有Joiner执行对齐同步
     * 同时，第二次进入该方法时，会等待Joiner对齐完成
     */
    private void processJoinerAlignment(CommonJoinUnionType<F, S> value, Context ctx, Collector<CommonJoinUnionType<F, S>> out) {
        lock.lock();
        try {
            //如果对齐已完成，则正常处理完元组后直接退出,系统可能执行到此的原因是一些线程同步的处理
            if (!routerState.isStartProcessJoinerAlignment()) {
                logger.info("CoModel-Router-" + subTaskNum + ":对齐早已完成！开始正常处理元组");
                //处理正常的元组
                processNormalTuples(value, ctx, out);
                return;
            }

            if (!routerState.isRouterCompleteSendAlignmentMess()) { //如果还没有向下游发送对齐消息，则发送对齐消息

                logger.info("CoModel-Router-" + subTaskNum + ":开始向下游Joiner发送对齐消息。。。");
                //处理正常的元组
                processNormalTuples(value, ctx, out);
                //向下游发送对齐消息
                sendAlignmentMessToAllJoiner(out);
                //改变Router状态，表明当前已经发送过对齐消息
                routerState.setRouterCompleteSendAlignmentMess(true);
                logger.info("CoModel-Router-" + subTaskNum + ":向下游Joiner发送对齐消息完毕！");

            } else {  //如果已经向下游发送过对齐消息，则线程阻塞，直到Joiner内存调整完毕

                //如果对齐未完成，则阻塞，直到对齐完成
                logger.info("CoModel-Router-" + subTaskNum + ":线程阻塞！等待Joiner内存变更同步操作执行完成。。。");
                //线程阻塞，直到中央协调通知Joiner变更内存操作执行完成
                wakeUpRouterCondition.await();

                logger.info("CoModel-Router-" + subTaskNum + ":线程被唤醒！开始正常处理元组");
                //处理正常的元组
                processNormalTuples(value, ctx, out);
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("CoModel-Router-" + subTaskNum + ":执行对齐操作失败！");
        }finally {
            lock.unlock();
        }

    }

    /**
     * 向下游所有的Joiner节点发送对齐消息
     */
    private void sendAlignmentMessToAllJoiner(Collector<CommonJoinUnionType<F, S>> out) {
        for (int i = 0; i < totalJoinerNum; i++) {
            CommonJoinUnionType<F, S> tuple = signalFactory.creatNotifyJoinerAlignmentMess(subTaskNum, i);
            out.collect(tuple);
        }
    }

    /**
     * 正常的处理元组，将元组转发到下游
     */
    private void processNormalTuples(CommonJoinUnionType<F, S> value, Context ctx, Collector<CommonJoinUnionType<F, S>> out) {
        //判断当前元组是R中元组还是S中的元组，分别加以处理
        if (value.isOne()) {
            processElementOf_R(value, ctx, out);
        } else {
            processElementOf_S(value, ctx, out);
        }
    }

    /**
     * 处理R流当中的元组
     */
    protected void processElementOf_R(CommonJoinUnionType<F, S> value, Context ctx, Collector<CommonJoinUnionType<F, S>> out) {
        //为当前元组设置序列号，并使序列号递增
        //--取余操作最好在前面，防止越界
        currentSerialNumOf_Store_R = (currentSerialNumOf_Store_R + 1) % R_totalStoreNum;
        value.setSerialNumOfCoModel(currentSerialNumOf_Store_R);

        //计算存储分区列表
        List<Integer> storePartitionNumList = getStorePartitionNumList(R_totalStoreNum, R_copiesNum, value.getSerialNumOfCoModel());
        //发送所有存储元组(R中的存储元组不加偏移量)
        for (int storeNum : storePartitionNumList) {
            CommonJoinUnionType<F, S> storeTuple = new CommonJoinUnionType<>();
            storeTuple.copyFrom(value);
            storeTuple.setStoreMode(true);
            storeTuple.setNumPartition(storeNum);  // 存储分区不加偏移量
            out.collect(storeTuple);
        }

        //计算连接分区列表(连接分区编号也是递增，不过其数量是与S流中的存储元组数量匹配的)
        currentSerialNumOf_Join_R = (currentSerialNumOf_Join_R + 1) % S_totalStoreNum;
        List<Integer> joinPartitionNumList = getJoinPartitionNumList(S_totalStoreNum, S_copiesNum, currentSerialNumOf_Join_R);
        //发送所有连接元组（R中的连接元组-需要-加偏移量）
        for (int joinNum : joinPartitionNumList) {
            CommonJoinUnionType<F, S> joinTuple = new CommonJoinUnionType<>();
            joinTuple.copyFrom(value);
            joinTuple.setJoinMode(true);
            joinTuple.setNumPartition(joinNum + R_totalStoreNum); // 连接分区-需要-加上偏移量
            out.collect(joinTuple);
        }
    }

    /**
     * 处理S流当中的元组
     */
    protected void processElementOf_S(CommonJoinUnionType<F, S> value, Context ctx, Collector<CommonJoinUnionType<F, S>> out) {
        //为当前元组设置序列号，并使序列号递增
        currentSerialNumOf_Store_S = (currentSerialNumOf_Store_S + 1) % S_totalStoreNum;
        value.setSerialNumOfCoModel(currentSerialNumOf_Store_S);

        //计算存储分区列表
        List<Integer> storePartitionNumList = getStorePartitionNumList(S_totalStoreNum, S_copiesNum, value.getSerialNumOfCoModel());
        //发送所有存储元组(S中的存储元组-需要-加偏移量)
        for (int storeNum : storePartitionNumList) {
            CommonJoinUnionType<F, S> storeTuple = new CommonJoinUnionType<>();
            storeTuple.copyFrom(value);
            storeTuple.setStoreMode(true);
            storeTuple.setNumPartition(storeNum + R_totalStoreNum);  // 存储分区-需要-加偏移量
            out.collect(storeTuple);
        }
        //计算连接分区列表(连接分区编号也是递增，不过其数量是与R流中的存储元组数量匹配的)
        currentSerialNumOf_Join_S = (currentSerialNumOf_Join_S + 1) % R_totalStoreNum;
        List<Integer> joinPartitionNumList = getJoinPartitionNumList(R_totalStoreNum, R_copiesNum, currentSerialNumOf_Join_S);
        //发送所有连接元组（S中的连接元组不加偏移量）
        for (int joinNum : joinPartitionNumList) {
            CommonJoinUnionType<F, S> joinTuple = new CommonJoinUnionType<>();
            joinTuple.copyFrom(value);
            joinTuple.setJoinMode(true);
            joinTuple.setNumPartition(joinNum); // S的连接分区不用加上偏移量
            out.collect(joinTuple);
        }

    }


    /**
     * 用于判断当前Router的状态的类
     */
    static class RouterStatus implements Serializable {
        // 当前是否处于要调整Joiner内存的状态中
        private boolean isStartProcessJoinerAlignment = false;
        // 当前Router是否完成了一次向下游Joiner发送对齐消息
        private boolean isRouterCompleteSendAlignmentMess = false;

        boolean isRouterCompleteSendAlignmentMess() {
            return isRouterCompleteSendAlignmentMess;
        }

        void setRouterCompleteSendAlignmentMess(boolean routerCompleteSendAlignmentMess) {
            isRouterCompleteSendAlignmentMess = routerCompleteSendAlignmentMess;
        }

        boolean isStartProcessJoinerAlignment() {
            return isStartProcessJoinerAlignment;
        }

        void setStartProcessJoinerAlignment(boolean startProcessJoinerAlignment) {
            isStartProcessJoinerAlignment = startProcessJoinerAlignment;
        }
    }
}
