package coModel;

import base.CommonJoinUnionType;
import base.CommonJoiner;
import base.subTreeForCoModel.DeleteConditionForSubTree;
import base.subTreeForCoModel.SubBPlusTreeForCoModel;
import coModel.tools.SignalMessageFactoryForCoModel;
import coModel.tools.sync.RichListenerWithoutReturn;
import coModel.tools.sync.TransferProtocolBasedZookeeper;
import coModel.tools.sync.ZookeeperBasedSynchronizer;
import coModel.tools.sync.ZookeeperNodePathSetForCoModel;
import common.GeneralParameters;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.util.Collector;
import org.apache.log4j.Logger;
import org.apache.zookeeper.WatchedEvent;

import java.io.Serializable;
import java.util.*;

/**
 * CoModel中的Joiner
 * @param <F> 第一个流的类型
 * @param <S> 第二个流的类型
 */
public class CoModelJoiner<F, S> extends CommonJoiner<F, S> {

    //系统中总的Router数量
    private int totalRouterNum = CoModelParameters.TOTAL_NUM_OF_ROUTER;

    //用于存储从协调器接收的最新的分区方案
    private Tuple4<Integer, Integer, Integer, Integer> newPartitionScheme;

    // 用于存储R中和S中元组的B+树集合，其中包含着若干子树
    private List<SubBPlusTreeForCoModel<CommonJoinUnionType<F, S>, Double>> R_StoreBPlusTreeSet;
    private List<SubBPlusTreeForCoModel<CommonJoinUnionType<F, S>, Double>> S_StoreBPlusTreeSet;

    //要用到zookeeper的同步器
    private ZookeeperBasedSynchronizer synchronizer;

    //当前子任务的编号
    private int subTaskIdx;

    //用于保存当前的水印
    private long currentWatermark = 0L;

    //用于保存上一次执行过期操作时的水位线
    private long lastExpireWatermark = 0L;

    //使用固定时间间隔的子窗口形式，每个子窗口的时间跨度
    private final long subTreeTimeWindowInterval = CoModelParameters.SUB_INDEX_TIME_INTERVAL;
    //执行子树过期的时间周期，在此设计为与子树的时间子窗口长度相同(ms)
    private final long expirePeriod = CoModelParameters.SUB_INDEX_TIME_INTERVAL;
    //系统最小的剩余内存阈值，若剩余内存小于该值，则判定系统内存过载
    private final int minRemainMemory = CoModelParameters.SYSTEM_MAX_REMAIN_MEMORY;

    //用于处理Router发送的信号消息的对象
    private final SignalMessageFactoryForCoModel<F, S> signalFactory = new SignalMessageFactoryForCoModel<>();

    //Joiner中要用到的定时器
    private final MyTimer timer = new MyTimer();
    //Joiner的定时器中定时执行的监控并处理内存变化的定时任务
    private MyTimerTask memoryMonitorTimerTask = null;

    //该子树中最多能够存储的元组数量，即系统不存在足够的空闲堆内存时当前Joiner中存储的元组数量，
    //--主要用于在系统的内存占用下降时增加副本数，优化处理延迟
    private long maxStoreTupleNum = -1L;

    //用于表示当前系统的状态
    private JoinerStatus joinerStatus = new JoinerStatus();

    //用于标识已经接收到哪些Router发送来的对齐消息，用于进行Joiner对齐
    private final HashSet<Integer> receivedAlignmentRouterSet = new HashSet<>();


    //获取logger对象用于日志记录
    private static final Logger logger = Logger.getLogger(CoModelJoiner.class.getName());


    /**
     * 有参构造器，进行范围连接的相关初始化参数设置
     */
    public CoModelJoiner(Time r_TimeWindows, Time s_TimeWindows, KeySelector<F, Double> keySelector_R, KeySelector<S, Double> keySelector_S, double r_surpass_S, double r_behind_S) {
        super(r_TimeWindows, s_TimeWindows, keySelector_R, keySelector_S, r_surpass_S, r_behind_S);
    }

    /**
     * 程序开始时运行一次
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        //参数初始化
        initParameters();

        //初始化存储结构
        initStoreDataStructureForBothStream();

        //初始化Zookeeper及相关配置
        initZookeeper();

        //初始化内存监视器
        startOrResetMemoryMonitorTimer(10000, 1000);

        //初始化Joiner需要用到的监听器
        initJoinerListeners();

        //打印配初始配置信息
        logJoinerInitConfiguration();
    }

    /**
     * 程序结束，清理所有组件
     */
    @Override
    public void close() throws Exception {
        synchronizer.close();
        timer.cancel();
    }

    /**
     * 启动Joiner时，初始化一些参数
     */
    private void initParameters() {
        //初始化当前任务编号
        subTaskIdx = getRuntimeContext().getIndexOfThisSubtask();
        logger.info("CoModel-Joiner-" + subTaskIdx + ":参数初始化成功！子窗口时间跨度为：" + subTreeTimeWindowInterval);
    }

    /**
     * 初始化Zookeeper及其相关配置,
     */
    private void initZookeeper() {
        synchronizer = new ZookeeperBasedSynchronizer(GeneralParameters.CONNECT_STRING, GeneralParameters.SESSION_TIMEOUT);
        logger.info("CoModel-Joiner-" + subTaskIdx + ":zookeeper同步器创建成功");
    }

    /**
     * 初始化用于存储两个流中元组的数据结构
     * --虽然在CoModel中每个Joiner只存储一个流中的元组，但是为了和其他方法兼容，以及便于后面的扩展，
     * --在此会同时初始两个存储结构，只是其中的一个流的存储结构中不会存储元组。
     */
    private void initStoreDataStructureForBothStream() {
        //初始化树的集合
        R_StoreBPlusTreeSet = new LinkedList<>();
        S_StoreBPlusTreeSet = new LinkedList<>();
        //初始化加入一个节点
        R_StoreBPlusTreeSet.add(new SubBPlusTreeForCoModel<>());
        S_StoreBPlusTreeSet.add(new SubBPlusTreeForCoModel<>());
    }

    /**
     * 启动Joiner时，打印一些系统的配置信息
     */
    private void logJoinerInitConfiguration() {
        //获取当前剩余的堆内存(单位：B)
        long currentFreeHeap = Runtime.getRuntime().freeMemory();
        logger.info("CoModel总Router数量为：" + CoModelParameters.TOTAL_NUM_OF_ROUTER
                + ";总Joiner数量为：" + CoModelParameters.TOTAL_NUM_OF_JOINER
                + ";其中，负责存储R流中元组的节点的数量为：" + CoModelParameters.TOTAL_R_JOINER_NUM
                + ";负责存储S流中元组的节点数量为：" + CoModelParameters.TOTAL_S_JOINER_NUM);
        logger.info("CoModel-Joiner-" + subTaskIdx + ":初始化堆内存为：" + (currentFreeHeap / 1024 / 1024) + "MB");
    }

    /**
     * 初始化Joiner内部用到的一些zookeeper监听器
     */
    private void initJoinerListeners() {
        //创建Joiner中用于监听一般性消息的zookeeper监听器
        synchronizer.creatLoopRichListenerWithoutReturn(
                ZookeeperNodePathSetForCoModel.JOINER_RECEIVE_COORDINATOR_COMMON_PREFIX + subTaskIdx,
                new RichListenerWithoutReturn() {
                    @Override
                    public void onTrigger(WatchedEvent event, byte[] data) {
                        logger.info("CoModel-Joiner-" + subTaskIdx + ":接收到一般性的通知消息，准备处理。。。");
                        processCommonZookeeperMess(event, data);
                    }
                });

        //创建Joiner中用于监听Monitor通知上传本地负载的监听器
        synchronizer.creatLoopRichListenerWithoutReturn(
                ZookeeperNodePathSetForCoModel.MONITOR_NOTIFY_JOINER_UPLOAD_WORKLOAD,
                new RichListenerWithoutReturn() {
                    @Override
                    public void onTrigger(WatchedEvent event, byte[] data) {
                        logger.info("CoModel-Joiner-" + subTaskIdx + ":接收Monitor通知上传本地负载的信息，准备上传负载。。。");
                        uploadLocalWorkloadToMonitor();
                    }
                });
    }

    /**
     * 将本地的负载上传给Monitor
     * 上传的负载形式为 -总子树数量，总元组数量，剩余堆内存（MB）-
     */
    private void uploadLocalWorkloadToMonitor() {
        //获取当前剩余的堆内存(单位：B)
        long currentFreeHeap = Runtime.getRuntime().freeMemory();

        //获取每个存储结构中存储的子树数量以及存储元组数量
        Tuple2<Long, Long> storeStatusOf_R = getStoreStatusOfEachStoreStructure(R_StoreBPlusTreeSet);
        Tuple2<Long, Long> storeStatusOf_S = getStoreStatusOfEachStoreStructure(S_StoreBPlusTreeSet);

        //上传的负载形式为 -总子树数量，总元组数量，剩余堆内存（MB）-
        String uploadWorkload = ""
                + (storeStatusOf_R.f0 + storeStatusOf_S.f0) + ","
                + (storeStatusOf_R.f1 + storeStatusOf_S.f1) + ","
                + (currentFreeHeap / 1024 / 1024);

        //上传负载信息
        synchronizer.setZookeeperNodeContent(
                ZookeeperNodePathSetForCoModel.JOINER_UPLOAD_ITSELF_WORKLOAD_TO_MONITOR_PREFIX + subTaskIdx,
                uploadWorkload.getBytes()
               );

        logger.info("CoModel-Joiner-" + subTaskIdx + ":上传本地负载成功！上传的负载为：" + uploadWorkload);

    }

    /**
     * Joiner中用于处理一般性zookeeper消息的方法
     * @param event zookeeper事件
     * @param data 节点内容
     */
    private void processCommonZookeeperMess(WatchedEvent event, byte[] data) {
        TransferProtocolBasedZookeeper receiveMess = new TransferProtocolBasedZookeeper(data);

        //如果接收的消息是上传的分区方案信息，则更新本地保存的最新分区方案
        if (receiveMess.isChangeCopiesNum()) {
            newPartitionScheme = receiveMess.getPartitionSchemeOfCoModel();
            logger.info("CoModel-Joiner-" + subTaskIdx + ":接收到中央协调器上传的最新存储分区方案:" + newPartitionScheme);
        }

        //如果是指示当前内存过载的消息
        if (receiveMess.isJoinerOverMemoryMess()) {
            logger.info("CoModel-Joiner-" + subTaskIdx + ":接收到中央协调器通知的表明本地内存过载的消息，将本地状态置为内存过载");
            joinerStatus.setMemoryStateNoticeReceived(true);
            joinerStatus.setMemoryOverLoad(true);
        }

        //TODO 如果是指示当前内存欠载的消息

    }

    /**
     * 初始化一个内存监视器，或者重置现有内存监视器的时间
     * @param initDelay 内存监视器第一次的执行时间(单位：ms)
     * @param period 内存监视器以后的周期性执行时间(单位：ms)
     */
    private void startOrResetMemoryMonitorTimer(long initDelay, long period) {
        //关闭上一个定时任务
        if (memoryMonitorTimerTask != null) {
            memoryMonitorTimerTask.cancel();
        }
        //新建定时任务
        memoryMonitorTimerTask = new MyTimerTask() {
            @Override
            public void run() {
                monitorMemoryResources(minRemainMemory);
            }
        };
        //调度新的定时任务
        timer.schedule(memoryMonitorTimerTask, initDelay, period);
        logger.info("CoModel-Joiner-" + subTaskIdx
                + ":初始化/重置了一个内存监控线程，线程将在：" + initDelay
                + " ms后第一次被执行，之后周期性执行时间为：" + period + "ms");
    }

    /**
     * 监控系统当前的内存占用，判断系统是否处于过载或欠载的状态
     * --若系统处于过载或欠载状态，则执行相应的操作
     * @param minFreeHeapMB 设定的最小剩余堆内存，当剩余堆内存小于该值时，系统开始执行内存调整操作（单位：MB）
     */
    private void monitorMemoryResources(long minFreeHeapMB) {

        //如果已经接收到中央协调器发送的表明内存状态的通知，则不再对内存状态做判断
        if (joinerStatus.isMemoryStateNoticeReceived()) {
            return;
        }

        //获取当前剩余堆内存（单位：B）
        long currentFreeHeap = Runtime.getRuntime().freeMemory();

        //当剩余内存小于指定值时，执行对应操作
        if (currentFreeHeap < (minFreeHeapMB * 1024 * 1024)) {

            logger.info("CoModel-Joiner-" + subTaskIdx
                    + ":系统剩余内存不足！指定最小内存阈值：" + minFreeHeapMB
                    + " MB,当前系统剩余内存为：" + currentFreeHeap
                    + "B(" + (currentFreeHeap / 1024 / 1024) + "MB).");

            //执行内存过载时对应的操作
            processWhenMonitorJoinerOverMemory();
        }

        //TODO 判断当占用内存过小时进行的相关系统欠载工作

    }

    /**
     * 在系统剩余的内存不足时，执行该方法以通知协调器当前节点过载
     * -上传中央协调器，表明自身内存不足(而自身内存的状态只在接收协调器消息时才会被设置)
     */
    private void processWhenMonitorJoinerOverMemory() {
        logger.info("CoModel-Joiner-" + subTaskIdx + ":执行剩余内存不足条件下的相关操作。。。");

        //上传中央协调器，表明自身内存不足
        TransferProtocolBasedZookeeper joinerOverMemoryMess = TransferProtocolBasedZookeeper.createJoinerOverMemoryMess("" + Runtime.getRuntime().freeMemory());
        synchronizer.setZookeeperNodeContent(
                ZookeeperNodePathSetForCoModel.JOINER_NOTIFY_COORDINATOR_COMMON_PREFIX + subTaskIdx,
                joinerOverMemoryMess.getTransferByteForThisMess());
        logger.info("CoModel-Joiner-" + subTaskIdx + ":成功上传本地内存过载信息！");
    }

    /**
     * 处理元组的入口
     */
    @Override
    public void processElement(CommonJoinUnionType<F, S> value, Context ctx, Collector<CommonJoinUnionType<F, S>> out) throws Exception {
        if (signalFactory.isSignalMess(value)) {  //处理信号消息
            processSignalMess(value);
        } else {  //处理正常元组
            processNormalInputTuple(value, ctx, out);
        }
    }

    /**
     * 处理由Router发送而来的信号消息
     * @param value 输入的信号消息
     */
    private void processSignalMess(CommonJoinUnionType<F, S> value) {
        //如果接收到的消息是对齐消息
        if (signalFactory.isAlignmentMess(value)) {
            processAlignmentMess(value);
        } else {
            logger.warn("CoModel-Joiner-" + subTaskIdx + ":警告！接收到未知的Router发送的消息类型！");
        }
    }

    /**
     * 处理对齐消息
     * @param value 传入的信号消息
     */
    private void processAlignmentMess(CommonJoinUnionType<F, S> value) {
        int routerIndex = signalFactory.getRouterIndexOfNotifyJoinerAlignmentMess(value);
        //如果接收到了全部Router发送的对齐消息，则执行内存变更操作
        receivedAlignmentRouterSet.add(routerIndex);
        if (receivedAlignmentRouterSet.size() == totalRouterNum) {
            logger.info("CoModel-Joiner-" + subTaskIdx + ":接收到全部Router的对齐消息，准备执行对齐后的操作。。。");
            //执行对齐后需要执行的操作
            executeOperationAfterAlignment();

            logger.info("CoModel-Joiner-" + subTaskIdx + ":执行对齐后的操作完成，准备重置系统的相关状态，并通知协调器Joiner执行完成。。。");

            //对齐操作执行完成后执行收尾操作
            finishOperationAfterAlignment();

            logger.info("CoModel-Joiner-" + subTaskIdx + ":所有与对齐有关的操作全部执行完成！接下来会正常处理之后到达的元组");
        }
    }

    /**
     * 当当前的Joiner接收到了上游所有的对齐消息之后，会调用该方法
     * --在该方法中执行内存的变更操作
     */
    private void executeOperationAfterAlignment() {

        if (joinerStatus.isMemoryOverLoad()) {  //当前Joiner内存过载，执行降低内存占用操作
            logger.info("CoModel-Joiner-" + subTaskIdx + ":状态为内存过载，准备减低系统内存占用。。。");
            //降低内存，触发垃圾回收
            reduceJoinerMemoryConsume();
            //重置定时器，20s内不再触发内存监测操作
            startOrResetMemoryMonitorTimer(20000,1000);
            logger.info("CoModel-Joiner-" + subTaskIdx + ":内存过载处理完成！已降低内存占用,现在剩余内存为："
                    + (Runtime.getRuntime().freeMemory() / 1024 / 1024) + " MB");

        } else if (joinerStatus.isMemoryUnderLoad()) { //TODO 当前Joiner内存欠载，执行增加内存占用操作
            logger.info("CoModel-Joiner-" + subTaskIdx + ":状态为内存欠载，准备增加系统内存占用。。。");
            increaseJoinerMemoryConsume();
            //重置定时器，20s内不再触发内存监测操作
            startOrResetMemoryMonitorTimer(20000,1000);
            logger.info("CoModel-Joiner-" + subTaskIdx + ":内存欠载处理完成！已增加内存占用,现在剩余内存为："
                    + (Runtime.getRuntime().freeMemory() / 1024 / 1024) + " MB");
        }else {
            logger.info("CoModel-Joiner-" + subTaskIdx + ":不处于任何要调整状态的状态中，不进行任何操作");
        }

    }

    /**
     * 降低当前Joiner的内存占用(删除所有不应被存储在本地的序列号的元组)，同时启动GC
     */
    private void reduceJoinerMemoryConsume() {
        //解析分区方案
        Integer storeNodeNumOf_R = newPartitionScheme.f0;
        Integer copiesNumOf_R = newPartitionScheme.f1;
        Integer storeNodeNumOf_S = newPartitionScheme.f2;
        Integer copiesNumOf_S = newPartitionScheme.f3;

        if (isLocalJoinerTheRStoreNode(newPartitionScheme, subTaskIdx)) {  //如果当前节点是R流的存储节点

            //获取系统存储R元组数量的上限
            Tuple2<Long, Long> storeStatus = getStoreStatusOfEachStoreStructure(R_StoreBPlusTreeSet);
            maxStoreTupleNum = storeStatus.f1;
            logger.info("CoModel-Joiner-" + subTaskIdx + ":该节点为-R-流存储节点，内存达到上限时，存储的-子树数量-以及-元组总数量-为：" + storeStatus);

            //获取当前节点应该保存元组的序列号范围
            Tuple2<Integer, Integer> localStoreSerialNumRange = getLocalStoreSerialNumRange(storeNodeNumOf_R, copiesNumOf_R, subTaskIdx);
            logger.info("CoModel-Joiner-" + subTaskIdx + ":该节点为-R-流存储节点，根据新的分区方案，本地需保存元组的序列号范围为：" + localStoreSerialNumRange);

            //删除所有本地不满足序列号范围的元组
            deleteAllOutOfRangeTuplesInSpecialSubTreeSet(localStoreSerialNumRange, R_StoreBPlusTreeSet);

        } else {  //如果当前节点是S流的存储节点

            //获取系统存储元组数量的上限
            Tuple2<Long, Long> storeStatus = getStoreStatusOfEachStoreStructure(S_StoreBPlusTreeSet);
            maxStoreTupleNum = storeStatus.f1;
            logger.info("CoModel-Joiner-" + subTaskIdx + ":该节点为-S-流存储节点，内存达到上限时，存储的-子树数量-以及-元组总数量-为：" + storeStatus);

            //获取当前节点应该保存元组的序列号范围
            Tuple2<Integer, Integer> localStoreSerialNumRange = getLocalStoreSerialNumRange(storeNodeNumOf_S, copiesNumOf_S, subTaskIdx - storeNodeNumOf_R);
            logger.info("CoModel-Joiner-" + subTaskIdx + ":该节点为-S-流存储节点，根据新的分区方案，本地需保存元组的序列号范围为：" + localStoreSerialNumRange);

            //删除所有本地不满足序列号范围的元组
            deleteAllOutOfRangeTuplesInSpecialSubTreeSet(localStoreSerialNumRange, S_StoreBPlusTreeSet);
        }

    }

    /**
     * 删除对应的子树集合中的所有不满足指定序列号范围的元组(主动调用GC)
     * @param localStoreSerialNumRange 所有应该被保存在本地的元组的序列号范围，格式为-最小序列号，最大序列号-，两端都包含
     * @param storeBPlusTreeSet 要被删除元组的子树集合
     */
    private void deleteAllOutOfRangeTuplesInSpecialSubTreeSet(
            Tuple2<Integer, Integer> localStoreSerialNumRange,
            List<SubBPlusTreeForCoModel<CommonJoinUnionType<F, S>, Double>> storeBPlusTreeSet) {
        long startTime = System.nanoTime();

        //删除每个子树中不满足序列号范围的元组
        for (SubBPlusTreeForCoModel<CommonJoinUnionType<F, S>, Double> subBPlusTree : storeBPlusTreeSet) {
            subBPlusTree.deleteNodeWithCondition(new DeleteConditionForSubTree<CommonJoinUnionType<F, S>>() {
                @Override
                public boolean isDeleted(CommonJoinUnionType<F, S> tuple) {
                    return tuple.getSerialNumOfCoModel() < localStoreSerialNumRange.f0 || tuple.getSerialNumOfCoModel() > localStoreSerialNumRange.f1;
                }
            });
        }

        //启动垃圾收集器
        System.gc();

        long finishTime = System.nanoTime();
        logger.info("CoModel-Joiner-" + subTaskIdx + ":删除本地的所有不在序列号范围内的元组成功！耗时（单位ms）：" + (finishTime - startTime) / 1000000);
    }

    /**
     * 根据指定流的分区方案以及该任务在对应流中的的编号，确定该任务需要保存的元组的序列号范围
     * @param storeNodeNum 对应流的存储节点数量（在我们的方案中，序列号的最大值为 storeNodeNum-1）
     * @param copiesNum 对应流的副本数量
     * @param relativeIndex 当前Joiner在对应流的存储节点中的相对编号，
     *                      如果当前节点是R的存储节点，则直接输入当前Joiner编号；
     *                      而若为S的存储节点，则需要用当前Joiner的编号减去R流存储节点的数量
     * @return 返回的序列号范围形式为 -最小序列号，最大序列号-
     *          在这两个序列号之间的元组才会被保存在本地（最小序列号与最大序列号都会被存储在本地，即边界都包括）
     */
    private Tuple2<Integer, Integer> getLocalStoreSerialNumRange(int storeNodeNum, int copiesNum, int relativeIndex) {
        //当前任务在所属流的第几组中
        int localTaskGroup = relativeIndex / copiesNum;
        //该任务需要存储的最小的序列号为对应组的初始点，最大的序列号为对应组的终点
        return new Tuple2<>(localTaskGroup * copiesNum, (localTaskGroup + 1) * copiesNum - 1);
    }

    /**
     * 判断当前Joiner是否是R流中元组的存储节点
     * @param partitionScheme 当前的分区方案
     * @param localTaskIndex 当前Joiner的编号
     * @return 如果本地Joiner是用于存储R流中的元组的，返回true
     */
    private boolean isLocalJoinerTheRStoreNode(Tuple4<Integer, Integer, Integer, Integer> partitionScheme, int localTaskIndex) {
        return localTaskIndex < partitionScheme.f0;
    }

    /**
     * TODO 增加当前Joiner的内存占用（以提升系统性能）
     */
    private void increaseJoinerMemoryConsume() {

    }

    /**
     * 在Joiner完成对齐以及执行完对齐后要执行的操作后，调用该方法
     * --该方法会完成一次对齐操作后的所有收尾操作，重置所有状态位,重置对齐标记集合
     * --同时该方法还会向协调器发送通知，通知当前同步回合已经结束
     */
    private void finishOperationAfterAlignment() {
        joinerStatus.setMemoryStateNoticeReceived(false);
        joinerStatus.setMemoryOverLoad(false);
        joinerStatus.setMemoryUnderLoad(false);
        receivedAlignmentRouterSet.clear();
        //向协调器发送消息，表明Joiner已经完成了内存更改
        synchronizer.respondOneToAllManySyncRound(
                ZookeeperNodePathSetForCoModel.C_R_J_CHANGE_JOINER_MEMORY_SYNC_ROUND,
                subTaskIdx,
                "JoinerChangeMemoryFinish".getBytes());
        logger.info("CoModel-Joiner-" + subTaskIdx + ":将Joiner的相关状态重置成功！并已经向协调器发送回合结束的通知！");
    }

    /**
     * 处理正常的元组，包括元组的插入，探测，过期以及输出等操作
     */
    private void processNormalInputTuple(CommonJoinUnionType<F, S> value, Context ctx, Collector<CommonJoinUnionType<F, S>> out) throws Exception {

        //元组存储
        if (value.isStoreMode()) {
            insertTuple(value);
        }

        //元组探测及输出
        if (value.isJoinMode()) {
            probeTupleAndOutput(value, ctx, out);
        }

        //元组过期
        currentWatermark = ctx.timerService().currentWatermark();
        if ((currentWatermark - lastExpireWatermark) > expirePeriod) {  //如果两次过期之间的间隔到达阈值，则执行过期
            lastExpireWatermark = currentWatermark;
            expireOperation(currentWatermark);
        }

    }


    /**
     * 将元组插入对应的存储结构中
     */
    private void insertTuple(CommonJoinUnionType<F, S> value) throws Exception {

        if (value.isOne()) {  //R流的存储元组
            Double key = keySelector_R.getKey(value.getFirstType());
            long selfTimestamp = value.getSelfTimestamp();
            insertTupleToEachStoreStructure(value, key, selfTimestamp, R_StoreBPlusTreeSet);
        } else {  //S流的存储元组
            Double key = keySelector_S.getKey(value.getSecondType());
            long selfTimestamp = value.getSelfTimestamp();
            insertTupleToEachStoreStructure(value, key, selfTimestamp, S_StoreBPlusTreeSet);
        }

    }

    /**
     * 将指定的元组插入到对应的存储结构中,当子窗口的时间跨度达到阈值时，新建子索引插入
     * @param value             输入元组
     * @param key               元组对应的键值
     * @param timestamp         元组对应的时间戳
     * @param storeBPlusTreeSet 要插入的存储结构
     */
    private void insertTupleToEachStoreStructure(CommonJoinUnionType<F, S> value,
                                                 Double key,
                                                 long timestamp,
                                                 List<SubBPlusTreeForCoModel<CommonJoinUnionType<F, S>, Double>> storeBPlusTreeSet) {
        //将当前元组插入当前活动的子树
        SubBPlusTreeForCoModel<CommonJoinUnionType<F, S>, Double> currentActiveSubTree = storeBPlusTreeSet.get(0);
        currentActiveSubTree.insert(value, key, timestamp);
        //如果当前活动子树的最大与最小时间戳之间的间隔达到阈值，则新建活动子树插入，插入位置恒定为0
        if (currentActiveSubTree.getMaxTimestamp() - currentActiveSubTree.getMinTimestamp() > subTreeTimeWindowInterval) {
            logger.info("CoModel-Joiner-" + subTaskIdx + ":新建了一个子索引，上一个已完成（归档）的子索引中的元组数量为" + currentActiveSubTree.getLength());
            storeBPlusTreeSet.add(0, new SubBPlusTreeForCoModel<>());
        }
    }

    /**
     * 将输入的元组和相对流的元组进行匹配，并输出所有连接结果
     */
    private void probeTupleAndOutput(CommonJoinUnionType<F, S> value, Context ctx, Collector<CommonJoinUnionType<F, S>> out) throws Exception {
        if (value.isOne()) {  // R中的元组，要与S的存储结构连接
            Double key = keySelector_R.getKey(value.getFirstType());
            long minTimestamp = value.getSelfTimestamp() - S_TimeWindows.toMilliseconds();
            double minKey = key - R_surpass_S;
            double maxKey = key + R_behind_S;

            List<CommonJoinUnionType<F, S>> resultList = getAllMatchedTuplesForEachStream(minTimestamp, minKey, maxKey, S_StoreBPlusTreeSet);
            matchInputWithResultTuplesAndOutput(value, resultList, out);

        } else {  // S中的元组，需要与R中的存储结构连接
            Double key = keySelector_S.getKey(value.getSecondType());
            long minTimestamp = value.getSelfTimestamp() - R_TimeWindows.toMilliseconds();
            double minKey = key - R_behind_S;
            double maxKey = key + R_surpass_S;

            List<CommonJoinUnionType<F, S>> resultList = getAllMatchedTuplesForEachStream(minTimestamp, minKey, maxKey, R_StoreBPlusTreeSet);
            matchInputWithResultTuplesAndOutput(value, resultList, out);

        } //end if
    }

    /**
     * 查找指定存储结构中所有在给定的时间和键值范围内的元组
     * @param minTimestamp 要连接元组的最小时间戳，由输入元组的时间戳和时间窗口大小决定
     * @param minKey 要查找的最小键值
     * @param maxKey 要查找的最大键值
     * @param storeBPlusTreeSet 要查找的存储结构
     * @return 返回所有满足时间条件和键值范围的元组
     */
    private List<CommonJoinUnionType<F, S>> getAllMatchedTuplesForEachStream(long minTimestamp,
                                                                             Double minKey,
                                                                             Double maxKey,
                                                                             List<SubBPlusTreeForCoModel<CommonJoinUnionType<F, S>, Double>> storeBPlusTreeSet) {
        ArrayList<CommonJoinUnionType<F, S>> resultList = new ArrayList<>();
        //遍历
        for (SubBPlusTreeForCoModel<CommonJoinUnionType<F, S>, Double> subTree : storeBPlusTreeSet) {
            if (subTree.getMaxTimestamp() < minTimestamp) {
                continue;
            }
            List<CommonJoinUnionType<F, S>> subResultList = subTree.findRange(minKey, maxKey, minTimestamp);
            if (subResultList != null) {
                resultList.addAll(subResultList);
            }
        }// end for

        return resultList;
    }


    /**
     * 将输入的元组和所有满足键值范围和时间范围的相对流的元组进行连接，并输出所有连接结果
     *
     * @param value 当前输入的元组
     * @param resultTupleList 通过匹配方法获取的所有满足时间范围和键值范围的相对流的元组
     * @param out 用于连接结果的输出
     */
    private void matchInputWithResultTuplesAndOutput(CommonJoinUnionType<F, S> value,
                                                     List<CommonJoinUnionType<F, S>> resultTupleList,
                                                     Collector<CommonJoinUnionType<F, S>> out) {
        // 连接结果的所有参数，如otherTimestamp等，均与新输入的元组相同
        for (CommonJoinUnionType<F, S> matchedTuple : resultTupleList) {
            out.collect(value.union(matchedTuple));
        }
    }


    /**
     * 执行过期操作，过期本地的所有存储结构
     * @param currentWatermark 当前的水位线
     */
    private void expireOperation(long currentWatermark) {

        logger.info("Joiner-" + subTaskIdx
                + "-开始执行过期操作，当前水位线为：" + currentWatermark
                + ";执行过期前R树中存储的<子树数，总元组数>为：" + getStoreStatusOfEachStoreStructure(R_StoreBPlusTreeSet)
                + ";执行过期前S树中存储的<子树数，总元组数>为：" + getStoreStatusOfEachStoreStructure(S_StoreBPlusTreeSet));

        //记录过期操作开始时间
        long startTime = System.currentTimeMillis();

        long minTimestampOf_R = currentWatermark - R_TimeWindows.toMilliseconds();
        long minTimestampOf_S = currentWatermark - S_TimeWindows.toMilliseconds();
        expireEachStoreStructure(R_StoreBPlusTreeSet, minTimestampOf_R);
        expireEachStoreStructure(S_StoreBPlusTreeSet, minTimestampOf_S);

        //记录过期操作结束时间
        long stopTime = System.currentTimeMillis();
        //输出过期操作所花费的时间
        logger.info("Joiner-" + subTaskIdx + "-过期操作完成！过期操作花费的时间（ms）为：" + (stopTime - startTime));

        logger.info("Joiner-" + subTaskIdx + "-过期操作完成"
                + ";执行过期后R树中存储的<子树数，总元组数>为：" + getStoreStatusOfEachStoreStructure(R_StoreBPlusTreeSet)
                + ";执行过期后S树中存储的<子树数，总元组数>为：" + getStoreStatusOfEachStoreStructure(S_StoreBPlusTreeSet));
    }

    /**
     * 获取对应存储结构中存储的子树数量以及总的存储元组的数量
     * @param storeBPlusTreeSet 要查询状态的子树
     * @return 一个二元组，其结构为 -子树数量，总元组数量-
     */
    private Tuple2<Long,Long> getStoreStatusOfEachStoreStructure(List<SubBPlusTreeForCoModel<CommonJoinUnionType<F, S>, Double>> storeBPlusTreeSet) {
        long subTreeNum = storeBPlusTreeSet.size();   //子树数量
        long totalTuplesNum = 0;    //总元组数量
        for (SubBPlusTreeForCoModel<CommonJoinUnionType<F, S>, Double> subTree : storeBPlusTreeSet) {
            totalTuplesNum += subTree.getLength();
        }
        return new Tuple2<>(subTreeNum, totalTuplesNum);
    }

//    /**
//     * 执行每个存储结构的过期操作，将对应存储结构的所有子树中最大时间戳小于该最小时间戳的子树删除
//     * --该方法中的删除方法是调用每一颗子树中的遍历删除方法，该方法会扫描所有的叶子节点然后依次删除，耗时巨大
//     * --该方法和下面另一个同名方法可以互换
//     * @param storeBPlusTreeSet 要执行过期操作的子树集
//     * @param coMinTimestamp 对应子树应当保存的最小时间戳，应该由当前水位线和时间窗口共同计算出，即：水位线 - 时间窗口长度
//     */
//    private void expireEachStoreStructure(List<SubBPlusTreeForCoModel<CommonJoinUnionType<F, S>, Double>> storeBPlusTreeSet, long coMinTimestamp) {
//
//        logger.info("Joiner-" + subTaskIdx + ":注意！当前程序的<过期方法>为每个B+子树遍历叶子节点进行删除的方式，可能耗时巨大！");
//
//        Iterator<SubBPlusTreeForCoModel<CommonJoinUnionType<F, S>, Double>> iterator = storeBPlusTreeSet.iterator();
//
//        while (iterator.hasNext()) {
//            //系统中应该至少有一个子索引，否则数组会越界
//            if (storeBPlusTreeSet.size() == 1) {
//                break;
//            }
//
//            //删除过期子索引
//            SubBPlusTreeForCoModel<CommonJoinUnionType<F, S>, Double> next = iterator.next();
//            if (next.getMaxTimestamp() < coMinTimestamp) {
//                next.clear();  //这步就是遍历子树中所有叶子节点，然后进行删除的代码，耗时应该会很大
//                iterator.remove();
//            }
//        }//end while
//
//    }

    /**
     * 执行每个存储结构的过期操作，将对应存储结构的所有子树中最大时间戳小于该最小时间戳的子树删除
     * --该方法直接弹出要删除的子树，不会对子树进行遍历，因此耗时应该会更小
     * @param storeBPlusTreeSet 要执行过期操作的子树集
     * @param coMinTimestamp 对应子树应当保存的最小时间戳，应该由当前水位线和时间窗口共同计算出，即：水位线 - 时间窗口长度
     */
    private void expireEachStoreStructure(List<SubBPlusTreeForCoModel<CommonJoinUnionType<F, S>, Double>> storeBPlusTreeSet, long coMinTimestamp) {

        Iterator<SubBPlusTreeForCoModel<CommonJoinUnionType<F, S>, Double>> iterator = storeBPlusTreeSet.iterator();

        while (iterator.hasNext()) {
            //系统中应该至少有一个子索引，否则数组会越界
            if (storeBPlusTreeSet.size() == 1) {
                break;
            }

            //删除过期子索引
            SubBPlusTreeForCoModel<CommonJoinUnionType<F, S>, Double> next = iterator.next();
            if (next.getMaxTimestamp() < coMinTimestamp) {
                iterator.remove();
            }
        }//end while

    }



    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {

    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {

    }

    /**
     * 用于指示当前Joiner状态的类
     */
    static class JoinerStatus implements Serializable {
        //标记内存过载
        private boolean isMemoryOverLoad = false;
        //标记内存欠载
        private boolean isMemoryUnderLoad = false;
        //标记是否接收到中央协调器下发的通知内存状态的消息
        private boolean isMemoryStateNoticeReceived = false;

        boolean isMemoryStateNoticeReceived() {
            return isMemoryStateNoticeReceived;
        }

        void setMemoryStateNoticeReceived(boolean memoryStateNoticeReceived) {
            isMemoryStateNoticeReceived = memoryStateNoticeReceived;
        }

        boolean isMemoryOverLoad() {
            return isMemoryOverLoad;
        }

        void setMemoryOverLoad(boolean memoryOverLoad) {
            isMemoryOverLoad = memoryOverLoad;
        }

        boolean isMemoryUnderLoad() {
            return isMemoryUnderLoad;
        }

        void setMemoryUnderLoad(boolean memoryUnderLoad) {
            isMemoryUnderLoad = memoryUnderLoad;
        }
    }

    /**
     * 可以序列化的定时器任务
     */
    static abstract class MyTimerTask extends TimerTask implements Serializable { }

    /**
     * 可以序列化的定时器
     */
    static class MyTimer extends Timer implements Serializable { }

}
