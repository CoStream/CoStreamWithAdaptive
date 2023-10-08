package coModel.adaptive;

import coModel.CoModelParameters;
import coModel.tools.sync.*;
import common.GeneralParameters;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.log4j.Logger;
import org.apache.zookeeper.WatchedEvent;
import tools.common.ParseExperimentParametersTool;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * CoModel模型的中央协调器，负责Router与Joiner之间的同步协调等操作
 */
public class CoModelCoordinatorWithAdaptive extends Thread implements Serializable {

    //用于判断系统是否存活的节点路径
    private String coModelSystemAliveNodePath = ZookeeperNodePathSetForCoModel.CO_MODEL_SYSTEM_ALIVE;

    //系统中Router与Joiner的总数量
    private int totalRouterNodeNum;
    private int totalJoinerNodeNum;
    //系统中R流的存储节点和S流的存储节点数量
    private int storeNodeNumOf_R;
    private int storeNodeNumOf_S;
    //R与S的存储节点中存储的元组的副本数量
    private int copiesNumOf_R;
    private int copiesNumOf_S;

    // R与S的窗口大小（单位：ms）
    private long windowSizeOf_R;
    private long windowSizeOf_S;

    // 协调器收集系统信息（包括Router的输入速率信息以及Joiner的内存占用信息）的周期（单位：ms）
    private long monitorInterval = CoModelParameterWithAdaptive.COORDINATOR_COLLECT_INFORMATION_PERIOD;

    // 负载较低的次数超过该值时才考虑增加复制数（防止波动），该值对R与S分别统计
    private int cumulativeLowMemNumOf_R = 0;
    private int cumulativeLowMemNumOf_S = 0;

    // 距离下次执行复制数判断的循环次数，该值用于控制在一个复制数调整后隔一段时间才会开始调整复制数
    private long untilNextChangePeriodNumOf_R = 0;
    private long untilNextChangePeriodNumOf_S = 0;
    // 最大的距离下次执行复制数判断的循环次数，上面的值初始化为下面这个值，且每次减一，减到0时开始下一次的调整
    private long MAX_UNTIL_NEXT_CHANGE_PERIOD_NUM;

    // 用于控制在增加复制数前需要等待的周期次数，这个值代表的时间应该至少为一个子窗的大小，否则会在子窗初始时错误的频繁增加复制数
    private int LOCAL_MAX_CUMULATIVE_NUM_OF_LOW_MEM;

    //每个子索引存储的字节数上限
    private long eachSubIndexStoreByteUpLimit = 0;

    // 记录目前一共改变复制数的次数
    private long totalChangeCopyNum = 0;


    //要用到zookeeper的同步器
    private ZookeeperBasedSynchronizer synchronizer;

    //标记协调器状态
    private CoordinatorStatus coordinatorStatus = new CoordinatorStatus();

    //用于线程同步的锁
    private final Lock lock = new ReentrantLock();
    //用于唤醒协调器的条件
    private Condition wakeUpCoordinatorCondition = lock.newCondition();
    //用于在协调器主线程中休眠一段时间，之后自动唤醒的条件
    //用于主线程等待其余Joiner上传自身的内存状态时让出锁
    private Condition mainTreadSleepCondition = lock.newCondition();

    //当前唤醒主线程的事件
    private TriggerEvent currentWakeUpEvent;

    //获取logger对象用于日志记录
    private static final Logger logger = Logger.getLogger(CoModelCoordinatorWithAdaptive.class.getName());

    @Override
    public void run() {
        //协调器启动时调用一次
        open();

        //协调器循环执行
        lock.lock();
        try {
            while (coordinatorStatus.isSystemAlive()) {
                Thread.sleep(monitorInterval);  // 每次调用之间间隔指定的时间
                process();  // 循环调用的处理方法
            }
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            lock.unlock();
        }

        logger.info("中央协调器被判定为死亡，准备执行关闭操作！");

        //协调器关闭时执行一次
        close();
    }

    /**
     * 在协调器启动时执行一次
     */
    private void open() {

        //初始化同步相关配置（zookeeper同步器以及相关的节点前缀创建）
        initZookeeper();

        //初始化系统分区方案的相关配置参数
        initParameters();

        //初始化协调器所需的所有事件监听器，监听相关的节点
        initEventMonitor();

        System.out.println("中央协调器(自适应)启动成功！");
        logger.info("中央协调器(自适应)启动成功！");
    }

    /**
     * 初始化系统相关的配置信息
     */
    private void initParameters() {

        // 获取保存在Zookeeper中的实验配置
        byte[] argsBytes = synchronizer.getZookeeperNodeContent(ParseExperimentParametersTool.ZOOKEEPER_EXP_CONFIGS_PATH_OF_ALL_MODEL);

        //系统中Router与Joiner的总数量
        //系统中R流的存储节点和S流的存储节点数量
        totalRouterNodeNum = CoModelParameters.TOTAL_NUM_OF_ROUTER;
        totalJoinerNodeNum = CoModelParameters.TOTAL_NUM_OF_JOINER;
        storeNodeNumOf_R = CoModelParameters.TOTAL_R_JOINER_NUM;
        storeNodeNumOf_S = CoModelParameters.TOTAL_S_JOINER_NUM;

        //R与S的存储节点中存储的元组的副本数量
        copiesNumOf_R = Integer.parseInt(
                ParseExperimentParametersTool
                        .parseExpParametersForNameWithinByteArray(argsBytes, ParseExperimentParametersTool.PARAM_NAME_OF_R_COPY_NUM));//CoModelParameters.INIT_R_NMU_OF_COPIES;
        copiesNumOf_S = Integer.parseInt(
                ParseExperimentParametersTool
                        .parseExpParametersForNameWithinByteArray(argsBytes, ParseExperimentParametersTool.PARAM_NAME_OF_S_COPY_NUM));//CoModelParameters.INIT_S_NMU_OF_COPIES;

        // Joiner中存储的时间窗口大小(单位：ms)
        windowSizeOf_R = GeneralParameters.WINDOW_SIZE * 1000;
        windowSizeOf_S = GeneralParameters.WINDOW_SIZE * 1000;

        // 获取再每次调整复制数之后间隔多少个循环后才开始下一轮调整
        long subWindowDuration = Math.max(windowSizeOf_R, windowSizeOf_S) / CoModelParameterWithAdaptive.NUM_OF_SUB_INDEX;  //子窗口的时间
        long waitNextAdjustDuration = subWindowDuration + Math.max(windowSizeOf_R, windowSizeOf_S);
        MAX_UNTIL_NEXT_CHANGE_PERIOD_NUM = waitNextAdjustDuration / monitorInterval;

        // 计算在增加复制数前至少要等待的周期数
        LOCAL_MAX_CUMULATIVE_NUM_OF_LOW_MEM = (int) (subWindowDuration / monitorInterval) + 1;

        //范围连接的范围
        double R_surpass_S = Double.parseDouble(
                ParseExperimentParametersTool.parseExpParametersForNameWithinByteArray(argsBytes,
                        ParseExperimentParametersTool.PARAM_NAME_OF_R_SURPASS_S));
        double R_behind_S = Double.parseDouble(
                ParseExperimentParametersTool.parseExpParametersForNameWithinByteArray(argsBytes,
                        ParseExperimentParametersTool.PARAM_NAME_OF_R_BEHIND_S));

        //获取当前实验程序的配置字符串
        String expArgsString = new String(argsBytes);

        //计算每个子索引存储字节数的上限阈值
        long eachSubIndexByteCapacity = CoModelParameters.MAX_BYTE_CONSUME_IN_EACH_JOINER / (CoModelParameterWithAdaptive.NUM_OF_SUB_INDEX + CoModelParameterWithAdaptive.REDUNDANCY_NUM_OF_SUB_INDEX);
        eachSubIndexStoreByteUpLimit = (long) (eachSubIndexByteCapacity * CoModelParameterWithAdaptive.MAX_SUB_INDEX_MEM_FOOTPRINT_UP_LIMIT_FRACTION);
        if (CoModelParameterWithAdaptive.MAX_SUB_INDEX_MEM_FOOTPRINT_UP_LIMIT > 0) {
            eachSubIndexStoreByteUpLimit = CoModelParameterWithAdaptive.MAX_SUB_INDEX_MEM_FOOTPRINT_UP_LIMIT;
        }

        //打印系统当前的配置信息
        logger.info("中央协调器:分区方案初始化成功！");
        logger.info("中央协调器:当前系统中每个Joiner设定的剩余内存下限为(MB)-" + CoModelParameters.SYSTEM_MAX_REMAIN_MEMORY);
        logger.info("中央协调器:程序的上下游算子之间元组刷新时间(ms)为：" + GeneralParameters.FLINK_OUTPUT_FLUSH_PERIOD);
        logger.info("中央协调器:每个Joiner实例中子窗口的时间跨度为（s）：" + (subWindowDuration / 1000));

        logCurrentSystemConfiguration();

        //在标准输出中打印系统相关配置，为的是显示的更加明显
        System.out.println("中央协调器:当前系统的配置为：R的存储节点数量为-" + storeNodeNumOf_R
                + "；R中每个元组保存的副本数量为-" + copiesNumOf_R
                + "；S的存储节点数量为-" + storeNodeNumOf_S
                + "；S中每个元组保存的副本数量为-" + copiesNumOf_S);
        System.out.println("中央协调器:当前系统中每个Joiner设定的剩余内存下限为(MB)-" + CoModelParameters.SYSTEM_MAX_REMAIN_MEMORY);
        System.out.println("中央协调器:程序的上下游算子之间元组刷新时间(ms)为：" + GeneralParameters.FLINK_OUTPUT_FLUSH_PERIOD);
        System.out.println("中央协调器:每个Joiner实例中子窗口的时间跨度为（s）：" + (subWindowDuration / 1000));
        System.out.println("中央协调器:范围连接的范围为：R_surpass_S-" + R_surpass_S + ";R_behind_S-" + R_behind_S);
        System.out.println("中央协调器:当前程序的输入配置为：" + expArgsString);
        System.out.println("中央协调器:每个Slot配置的最大存储元组容量（M）：" + (CoModelParameters.MAX_BYTE_CONSUME_IN_EACH_JOINER / 1024 / 1024));
        System.out.println("中央协调器：当前实验的R的窗口总大小(所有子窗口的和，单位：毫秒)为" + windowSizeOf_R);
        System.out.println("中央协调器：当前实验的S的窗口总大小(所有子窗口的和，单位：毫秒)为" + windowSizeOf_S);
        System.out.println("中央协调器：设置的用于调整复制数的每个子索引存储的字节上限(单位：字节)阈值为" + eachSubIndexStoreByteUpLimit);
        System.out.println("中央协调器：是否开启自适应（True：开启）：" + CoModelParameterWithAdaptive.IS_ADAPTIVE_ENABLE);
        System.out.println("中央协调器：中央协调器的监控周期为（ms）：" + monitorInterval);
        System.out.println("中央协调器：在增加复制数前至少要等待的周期数（防止错误的多次迁移）：" + LOCAL_MAX_CUMULATIVE_NUM_OF_LOW_MEM);
        System.out.println("中央协调器：当前程序中Router的数量为：" + totalRouterNodeNum);
    }


    /**
     * 初始化所有需要的事件监听器
     */
    private void initEventMonitor() {
        //判断系统是否存活的事件监听器
        if (synchronizer.isNodeExists(coModelSystemAliveNodePath)) {
            coordinatorStatus.setSystemAlive(true);
            //时刻监视系统存活标记，当标记消失时，判定系统死亡
            synchronizer.creatOnceListenerWithoutReturn(coModelSystemAliveNodePath, new SyncListenerWithoutReturn() {
                @Override
                public void onTrigger(WatchedEvent event) {
                    coordinatorStatus.setSystemAlive(false);
                    wakeUpCoordinator(new TriggerEvent(WakeUpCoordinatorEventType.DEAD, event.getPath(), null));
                }
            });
            logger.info("中央协调器-系统存活监视器-初始化成功！");
        } else {
            coordinatorStatus.setSystemAlive(false);
            logger.info("中央协调器-初始即判断系统死亡！系统未成功启动");
        }

        //用于监视Joiner上传的一般事件的监听器（在同步块中调用要执行的方法）
        for (int i = 0; i < totalJoinerNodeNum; i++) {
            synchronizer.creatLoopRichListenerWithoutReturn(ZookeeperNodePathSetForCoModel.JOINER_NOTIFY_COORDINATOR_COMMON_PREFIX + i, new RichListenerWithoutReturn() {
                @Override
                public void onTrigger(WatchedEvent event, byte[] data) {
                    creatNewThreadToProcessCommonJoinerMess(event, data);
                }
            });
        }
        logger.info("中央协调器-Joiner监视器-初始化成功！");

    }

    /**
     * 为了不占用zookeeper的线程，新建线程去处理到达事件
     */
    private void creatNewThreadToProcessCommonJoinerMess(WatchedEvent event, byte[] data) {
        logger.info("中央协调器-新建线程来处理到达的一般Joiner事件");
        new Thread() {
            @Override
            public void run() {
                lock.lock();
                try {
                    processCommonMessOfJoinerListener(event, data);
                } catch (Exception e) {
                    e.printStackTrace();
                }finally {
                    lock.unlock();
                }
            }
        }.start();

    }

    /**
     * 根据一些条件，判断本次复制数计算是否执行，条件如下：
     *      1）程序是否开启自适应能力
     * @return True：执行
     */
    private boolean isCurrentProcessRequired() {
        // 1)若未开启自适应能力，则直接退出
        if (!CoModelParameterWithAdaptive.IS_ADAPTIVE_ENABLE) {
            return false;
        }

        return true;
    }

    /**
     * 在每次周期执行时，执行一些规律性的操作，如下：
     *  1） 将距离下次调整的等待周期减一
     */
    private void changeInEachPeriod() {
        if (untilNextChangePeriodNumOf_R > 0) {
            untilNextChangePeriodNumOf_R--;
        }
        if (untilNextChangePeriodNumOf_S > 0) {
            untilNextChangePeriodNumOf_S--;
        }
    }

    /**
     * 协调器启动后实际的执行逻辑,该方法周期性执行
     */
    private void process() {

        // 如果本次执行不被需要，则直接退出
        if (!isCurrentProcessRequired()) {
            return;
        }

        // 每个周期都需要执行的规律性操作
        changeInEachPeriod();

        //收集Router上的输入速率信息以及Joiner的内存占用信息
        List<String> allInputRate = collectInputRateFromRouters();
        List<String> allMemoryFootprint = collectMemoryFootprintFromJoiners();

        //计算新的理论上的复制数
        Tuple2<Integer, Integer> newCopyNumPairs = calculateAllNewCopyNum(allInputRate, allMemoryFootprint, copiesNumOf_R, copiesNumOf_S);

        //获取实际上的复制数
        Tuple2<Integer, Integer> actualNewCopyNum = getActualNewCopyNum(newCopyNumPairs, copiesNumOf_R, copiesNumOf_S);

        // 若距离上一次调整剩余的周期数未达标，则本次不进行调整
        if (untilNextChangePeriodNumOf_R > 0) {
            logger.info("中央协调器：距离下次调整R的复制数还剩周期数：" + untilNextChangePeriodNumOf_R);
            actualNewCopyNum.f0 = copiesNumOf_R;
        }
        if (untilNextChangePeriodNumOf_S > 0) {
            logger.info("中央协调器：距离下次调整S的复制数还剩周期数：" + untilNextChangePeriodNumOf_S);
            actualNewCopyNum.f1 = copiesNumOf_S;
        }

        //如果新计算的复制数与原来的不一样，则执行复制数调整的同步过程
        if ((actualNewCopyNum.f0 != copiesNumOf_R) || (actualNewCopyNum.f1 != copiesNumOf_S)) {

            totalChangeCopyNum++;
            logger.info("中央协调器：新计算出的复制数与原来的不一样，现在开始同步进行复制数的调整！当前已进行的改变复制数次数为：" + totalChangeCopyNum);
            logger.info("中央协调器：原来的R与S的复制数为：R：" + copiesNumOf_R + ";S:" + copiesNumOf_S + ";新的R与S的复制数为：" + actualNewCopyNum);
            System.out.println("中央协调器：新计算出的复制数与原来的不一样，现在开始同步进行复制数的调整！");
            System.out.println("中央协调器：原来的R与S的复制数为：R：" + copiesNumOf_R + ";S:" + copiesNumOf_S + ";新的R与S的复制数为：" + actualNewCopyNum);

            // 进行同步，并将新的复制数发送给所有的Router和Joiner
            uploadPartitionSchemeAndSyncJoinerMemoryChange(storeNodeNumOf_R, actualNewCopyNum.f0, storeNodeNumOf_S, actualNewCopyNum.f1);

            // 如果未开启复制数调整期间可以多次调整的功能，则等待一段时间之后再进行下次复制数调整
            // R 与 S的等待时间分别计算
            if (!CoModelParameterWithAdaptive.IS_MULTIPLE_ADJUST_ENABLE) {
                logger.info("中央协调器：本地复制数调整完成，调整后的复制数分别为R:" + copiesNumOf_R + ";S:" + copiesNumOf_S);
                if (actualNewCopyNum.f0 != copiesNumOf_R) {
                    untilNextChangePeriodNumOf_R = MAX_UNTIL_NEXT_CHANGE_PERIOD_NUM;
                    logger.info("中央协调器：由于未开启多次调整，且本次R的复制数已被调整，则距离下次调整R的复制数还剩周期数为：" + untilNextChangePeriodNumOf_R);
                }

                if (actualNewCopyNum.f1 != copiesNumOf_S) {
                    untilNextChangePeriodNumOf_S = MAX_UNTIL_NEXT_CHANGE_PERIOD_NUM;
                    logger.info("中央协调器：由于未开启多次调整，且本次S的复制数已被调整，则距离下次调整S的复制数还剩周期数为：" + untilNextChangePeriodNumOf_S);
                }

            }

            // 用新的复制数代替本地的复制数
            copiesNumOf_R = actualNewCopyNum.f0;
            copiesNumOf_S = actualNewCopyNum.f1;

        }

    }

    /**
     * 计算新的复制数
     * @param allInputRate 所有Router发送的输入速率信息
     * @param allMemoryFootprint 所有Joiner发送的子索引内存占用信息
     * @param currentCopyNumOf_R 当前系统的R的复制数
     * @param currentCopyNumOf_S 当前系统的S的复制数
     * @return 新的R的复制数，新的S的复制数
     */
    private Tuple2<Integer, Integer> calculateAllNewCopyNum(List<String> allInputRate, List<String> allMemoryFootprint, int currentCopyNumOf_R, int currentCopyNumOf_S) {
        // R的速率，S的速率
        Tuple2<Long, Long> inputRate = countInputRate(allInputRate);

        // 如果速率为0，则本次不调整
        if ((inputRate.f0 <= 0) || (inputRate.f1 <= 0)) {
            if (CoModelParameterWithAdaptive.IS_COORDINATOR_LOG_OPEN) {
                System.out.println("中央协调器：统计的输入速率为0，则新的复制数还是原值，跳过本次调整。当前系统的输入速率（R与Ｓ）为：" + inputRate);
            }
            //返回原来的复制数
            return new Tuple2<>(currentCopyNumOf_R, currentCopyNumOf_S);
        }

        // 将所有Joiner的内存占用拆分为R和S各自的存储节点的字符串
        ArrayList<String> allMemFootprintOf_R = new ArrayList<>(storeNodeNumOf_R);
        ArrayList<String> allMemFootprintOf_S = new ArrayList<>(storeNodeNumOf_S);
        for (int i = 0; i < totalJoinerNodeNum; i++) {
            if (i < storeNodeNumOf_R) {
                allMemFootprintOf_R.add(allMemoryFootprint.get(i));
            } else {
                allMemFootprintOf_S.add(allMemoryFootprint.get(i));
            }
        }

        // 所有Joiner当前活动子索引的最大值，当前活动子索引和上一个归档子索引的最大值的最大值，所有Joiner的内存占用的平均值
        Tuple3<Long, Long, Long> processedMemFootprintOf_R = countMemoryFootprint(allMemFootprintOf_R);
        Tuple3<Long, Long, Long> processedMemFootprintOf_S = countMemoryFootprint(allMemFootprintOf_S);

        // 计算R和S的新的复制数
        int newCopyNumOf_R = calculateEachNewCopyNum(processedMemFootprintOf_R, currentCopyNumOf_R, storeNodeNumOf_R, inputRate.f0, inputRate.f1);
        int newCopyNumOf_S = calculateEachNewCopyNum(processedMemFootprintOf_S, currentCopyNumOf_S, storeNodeNumOf_S, inputRate.f1, inputRate.f0);

        // 打印一些调试信息
        if (CoModelParameterWithAdaptive.IS_COORDINATOR_LOG_OPEN) {
            logSomeInformationPeriod(inputRate, processedMemFootprintOf_R, processedMemFootprintOf_S, newCopyNumOf_R, newCopyNumOf_S);
        }

        //返回结果
        return new Tuple2<>(newCopyNumOf_R, newCopyNumOf_S);
    }

    /**
     * 每次周期性计算相关结果时，打印若干信息
     * @param inputRate                 R的速率，S的速率
     * @param processedMemFootprintOf_R R 的所有Joiner当前活动子索引的最大值，当前活动子索引和上一个归档子索引的最大值的最大值，所有Joiner的内存占用的平均值
     * @param processedMemFootprintOf_S S 的所有Joiner当前活动子索引的最大值，当前活动子索引和上一个归档子索引的最大值的最大值，所有Joiner的内存占用的平均值
     * @param newCopyNumOf_R 新计算出的理论上的R的复制数
     * @param newCopyNumOf_S 新计算出的理论上的S的复制数
     */
    private void logSomeInformationPeriod(Tuple2<Long, Long> inputRate,
                                          Tuple3<Long, Long, Long> processedMemFootprintOf_R,
                                          Tuple3<Long, Long, Long> processedMemFootprintOf_S,
                                          int newCopyNumOf_R,
                                          int newCopyNumOf_S) {

        System.out.println("中央协调器：统计的R与S的输入速率为：" + inputRate
                + ";新计算出的理论上的R的复制数为：" + newCopyNumOf_R + ";理论上S的复制数为：" + newCopyNumOf_S);
        System.out.println("中央协调器：R与S的所有Joiner当前活动子索引的最大值，当前活动子索引和上一个归档子索引的最大值的最大值，所有Joiner的内存占用的平均值分别为："
                + processedMemFootprintOf_R + "; " + processedMemFootprintOf_S);
    }

    /**
     * 计算实际上的最优复制数
     * 调用该方法的原因是虽然降低复制数的决定可以直接执行，但是增加复制数的决定需要等一段时间后才能执行，这是为了防止波动
     * @param optimalCopyNumPair 理论上的最优复制数
     * @param currentCopyNumOf_R 当前系统的R的复制数
     * @param currentCopyNumOf_S 当前系统的S的复制数
     * @return 实际上的最优复制数
     */
    private Tuple2<Integer, Integer> getActualNewCopyNum(Tuple2<Integer, Integer> optimalCopyNumPair, int currentCopyNumOf_R, int currentCopyNumOf_S) {
        int newCopyNumOf_R = currentCopyNumOf_R;
        int newCopyNumOf_S = currentCopyNumOf_S;

        // 如果复制数降低，则直接采用降低后的值(同时重置对应的表明连续多少次监控到低内存占用的计数)
        if (optimalCopyNumPair.f0 < currentCopyNumOf_R) {
            cumulativeLowMemNumOf_R = 0;
            newCopyNumOf_R = optimalCopyNumPair.f0;
        }
        if (optimalCopyNumPair.f1 < currentCopyNumOf_S) {
            cumulativeLowMemNumOf_S = 0;
            newCopyNumOf_S = optimalCopyNumPair.f1;
        }

        // 如果复制数不变，则置对应的表明连续多少次监控到低内存占用的计数
        if (optimalCopyNumPair.f0 == currentCopyNumOf_R) {
            cumulativeLowMemNumOf_R = 0;
        }
        if (optimalCopyNumPair.f1 == currentCopyNumOf_S) {
            cumulativeLowMemNumOf_S = 0;
        }

        // 如果复制数增加，则等待一定的次数后才执行，
        // 且在一个流的复制数增加时，若另一个也连续几次出现低内存占用，则顺便一起增加，以降低同步开销
        if (optimalCopyNumPair.f0 > currentCopyNumOf_R) {
            cumulativeLowMemNumOf_R++;

            if (cumulativeLowMemNumOf_R > LOCAL_MAX_CUMULATIVE_NUM_OF_LOW_MEM) {  // 已经连续监测到结果,这个值应该至少超过一个子窗的时间

                logger.info("中央协调器：连续 " + cumulativeLowMemNumOf_R + " 次监控到R的低内存占用，因此准备增加复制数。");
                newCopyNumOf_R = optimalCopyNumPair.f0;
                cumulativeLowMemNumOf_R = 0;

                if (cumulativeLowMemNumOf_S > (4 * LOCAL_MAX_CUMULATIVE_NUM_OF_LOW_MEM / 5)) {  // 顺便判断S(达到百分之八十就认为也应该进行改变)
                    logger.info("中央协调器：在增加R的复制数时，连续 " + cumulativeLowMemNumOf_S + " 次监控到S的低内存占用，顺便准备增加S复制数。");
                    newCopyNumOf_S = optimalCopyNumPair.f1;
                    cumulativeLowMemNumOf_S = 0;
                    currentCopyNumOf_S = newCopyNumOf_S; // 加上这句是此时防止下面的调整S的复制数的方法再次被调用
                }
            } // 已经连续监测到低内存占用

        }

        if (optimalCopyNumPair.f1 > currentCopyNumOf_S) {
            cumulativeLowMemNumOf_S++;

            if (cumulativeLowMemNumOf_S > LOCAL_MAX_CUMULATIVE_NUM_OF_LOW_MEM) {  // 已经连续监测到结果
                logger.info("中央协调器：连续 " + cumulativeLowMemNumOf_S + " 次监控到S的低内存占用，因此准备增加复制数。");
                newCopyNumOf_S = optimalCopyNumPair.f1;
                cumulativeLowMemNumOf_S = 0;

                if (cumulativeLowMemNumOf_R > (4 * LOCAL_MAX_CUMULATIVE_NUM_OF_LOW_MEM / 5)) {  // 顺便判断R(达到百分之八十就认为也应该进行改变)
                    logger.info("中央协调器：在增加S的复制数时，连续 " + cumulativeLowMemNumOf_R + " 次监控到R的低内存占用，顺便准备增加R复制数。");
                    newCopyNumOf_R = optimalCopyNumPair.f0;
                    cumulativeLowMemNumOf_R = 0;
                }
            } // 已经连续监测到低内存占用

        }

        return new Tuple2<>(newCopyNumOf_R, newCopyNumOf_S);
    }


    /**
     * 计算一个流的新的复制数，两个流分别调用一次该方法
     * @param processedMemFootprint 该流对应的所有存储节点的内存占用情况，各项如下：
     *                              1）所有Joiner当前活动子索引的最大值，
     *                              2）当前活动子索引和上一个归档子索引的最大值的最大值，
     *                              3）所有Joiner的内存占用的平均值
     * @param lastCopyNum 该流对应的上次的复制数
     * @param storeNodeNum 该流的存储节点数量
     * @param currentStreamInputRate 该流的流速
     * @param opStreamInputRate 该流相对的另一个流的流速
     * @return 该流的新的复制数
     */
    private int calculateEachNewCopyNum(Tuple3<Long, Long, Long> processedMemFootprint, int lastCopyNum, int storeNodeNum, long currentStreamInputRate, long opStreamInputRate) {

        if (processedMemFootprint.f0 >= eachSubIndexStoreByteUpLimit) {
            if (lastCopyNum == 1) {
                return 1;
            } else {
                logger.info("中央协调器：某流内存过限,现复制数降为原来的一半，即变为：" + (lastCopyNum / 2));
                return lastCopyNum / 2;
            }
        }

        // 新计算出的复制数
        double optimalCopyNum = Math.sqrt((1.0 * opStreamInputRate * storeNodeNum) / currentStreamInputRate);

        // 如果新计算的复制数小于上个复制数，则降低复制数，这是因为要求的复制数是小于最优复制数的一个最大的2的幂
        if (optimalCopyNum < lastCopyNum) {
            if (lastCopyNum == 1) {
                return 1;
            } else {
                return lastCopyNum / 2;
            }
        }

        // 如果新计算的复制数大于等于上个复制数的二倍，则倍增复制数，这也是因为要求的复制数是小于最优复制数的一个最大的2的幂
        if (optimalCopyNum >= (2 * lastCopyNum)) {
            if (processedMemFootprint.f1 < (eachSubIndexStoreByteUpLimit / 2)) {
                return 2 * lastCopyNum;
            } else {
                return lastCopyNum;
            }
        }

        return lastCopyNum;
    }

    /**
     * 获取所有Router发送的输入速率信息
     * @return 所有Router的输入速率信息
     */
    private List<String> collectInputRateFromRouters() {
        ArrayList<String> result = new ArrayList<>(totalRouterNodeNum);
        for (int i = 0; i < totalRouterNodeNum; i++) {
            byte[] zookeeperNodeContent = synchronizer.getZookeeperNodeContent(ZookeeperNodePathSetForCoModel.ROUTER_MONITOR_TWO_STREAM_INPUT_RATE_PREFIX + i);
            result.add(new String(zookeeperNodeContent));
        }
        return result;
    }

    /**
     * 统计总的输入速率
     * @param allInputRate 所有Router上传的各自统计的输入速率，即collectInputRateFromRouters返回的结果
     * @return R的速率，S的速率
     */
    private Tuple2<Long, Long> countInputRate(List<String> allInputRate) {
        long R_InputRate = 0;
        long S_InputRate = 0;
        for (String s : allInputRate) {
            String[] split = s.split(",");
            R_InputRate += Long.parseLong(split[0]);
            S_InputRate += Long.parseLong(split[1]);
        }
        return new Tuple2<>(R_InputRate, S_InputRate);
    }


    /**
     * 获取所有Joiner发送的子索引内存占用信息
     * @return 所有Joiner发送的子索引内存占用信息
     */
    private List<String> collectMemoryFootprintFromJoiners() {
        ArrayList<String> result = new ArrayList<>(totalJoinerNodeNum);
        for (int i = 0; i < totalJoinerNodeNum; i++) {
            byte[] zookeeperNodeContent = synchronizer.getZookeeperNodeContent(ZookeeperNodePathSetForCoModel.JOINER_UPLOAD_SUB_INDEX_MEM_COMMON_PREFIX + i);
            result.add(new String(zookeeperNodeContent));
        }
        return result;
    }

    /**
     * 获取所有的Joiner的内存占用的统计信息，用于调整复制数（对于R和S的存储节点分别调用一次）
     * @param allMemFootprint 所有Joiner上传的各自的内存占用，即collectMemoryFootprintFromJoiners的返回结果
     * @return 所有Joiner当前活动子索引的最大值，当前活动子索引和上一个归档子索引的最大值的最大值，所有Joiner的内存占用的平均值
     */
    private Tuple3<Long, Long, Long> countMemoryFootprint(List<String> allMemFootprint) {
        long maxActiveIndexMem = 0;
        long maxActiveAndArchiveIndexMem = 0;
        long totalMemoryFootprint = 0;
        for (String s : allMemFootprint) {
            String[] split = s.split(",");
            long currentActiveMem = Long.parseLong(split[0]);
            long currentArchiveMem = Long.parseLong(split[1]);
            long currentTotalMem = Long.parseLong(split[2]);
            // 获取最大活动子索引内存占用
            if (currentActiveMem > maxActiveIndexMem) {
                maxActiveIndexMem = currentActiveMem;
            }
            // 获取当前活动子索引和上一个归档子索引的最大值的最大值
            if (Math.max(currentActiveMem, currentArchiveMem) > maxActiveAndArchiveIndexMem) {
                maxActiveAndArchiveIndexMem = Math.max(currentActiveMem, currentArchiveMem);
            }
            //累加总内存占用
            totalMemoryFootprint += currentTotalMem;
        }

        return new Tuple3<>(maxActiveIndexMem, maxActiveAndArchiveIndexMem, (totalMemoryFootprint / allMemFootprint.size()));
    }

    /**
     * 将指定的存储节点数以及复制数同步给所有的Router以及Joiner，并协调Router与Joiner之间进行的对齐等同步操作
     * 发起Joiner内存变更回合，包括：
     * （1）上传最新的分区方案给Joiner和Router
     * （2）通知Router开始进行Joiner对齐消息的发送
     * （3）Joiner对齐由Joiner发送的对齐消息
     * （4）Joiner变更内部的存储结构（减少内存/增加内存）
     * （5）Joiner通知协调器变更内存完成
     * （6）协调器通知Router内存变更完成，Router可以正确处理数据
     */
    private void uploadPartitionSchemeAndSyncJoinerMemoryChange(int storeNodeNumOf_R, int copiesNumOf_R, int storeNodeNumOf_S, int copiesNumOf_S) {
        //最新的分区方案
        Tuple4<Integer, Integer, Integer, Integer> partitionScheme
                = new Tuple4<>(storeNodeNumOf_R, copiesNumOf_R, storeNodeNumOf_S, copiesNumOf_S);
        logger.info("中央协调器-开始上传最新的分区方案，并开启Joiner内存变更回合，上传的分区方案(storeNodeNumOf_R, copiesNumOf_R, storeNodeNumOf_S, copiesNumOf_S)为：" + partitionScheme);

        TransferProtocolBasedZookeeper changeCopiesNumMess = TransferProtocolBasedZookeeper.createChangeCopiesNumMess(partitionScheme);

        //上传分区方案给所有的Joiner
        logger.info("中央协调器-Joiner将新的分区方案上传给所有的Joiner");
        synchronizer.setAllMultipleInstancesNodeContent(
                ZookeeperNodePathSetForCoModel.JOINER_RECEIVE_COORDINATOR_COMMON_PREFIX,
                totalJoinerNodeNum,
                changeCopiesNumMess.getTransferByteForThisMess());

        //开始回合同步（同时也是将分区方案上传给所有的Router）
        logger.info("中央协调器-Joiner内存变更回合开始，等待同步完成。。。");
        synchronizer.initiateOneToAllManySyncRoundAndSyncWait(
                ZookeeperNodePathSetForCoModel.C_R_J_CHANGE_JOINER_MEMORY_SYNC_ROUND,
                totalJoinerNodeNum,
                changeCopiesNumMess.getTransferByteForThisMess());
        logger.info("中央协调器-Joiner内存变更回合结束，同步成功！");

        //通知Router内存变更回合结束
        synchronizer.setZookeeperNodeContent(
                ZookeeperNodePathSetForCoModel.ROUTER_RECEIVE_COORDINATOR_COMMON_NODE,
                TransferProtocolBasedZookeeper.createNotifyJoinerSyncCompleteMess().getTransferByteForThisMess());
        logger.info("中央协调器-通知Router同步结束完成，其可以开始元组的正常处理");
    }

//    /**
//     * 处理Joiner内存变更事件（包括内存过载与欠载）
//     * @param currentWakeUpEvent 当前的事件
//     */
//    private void processJoinerChangeMemory(TriggerEvent currentWakeUpEvent) {
//        logger.info("中央协调器-开始处理Joiner节点的内存变更事件");
//        lock.lock();
//        try {
//            //等待一段时间，等待两个流的Joiner都进行完内存判断，尽量令两个流在同一个同步周期中改变内存
//            //该方法会释放锁，并在一段时间之后自动被执行
//            mainTreadSleepCondition.await(5000, TimeUnit.MILLISECONDS);
//
//            //用于指示R或者S的内存是否被处理过，用于解决一些与同步有关的问题
//            boolean is_R_Processed = false;
//            boolean is_S_Processed = false;
//
//            //处理R流中的内存过载
//            //TODO 在此需要考虑副本数为0的情况，现在还没考虑到
//            if (coordinatorStatus.isProcessRMemoryOver()) {
//                is_R_Processed = true;
//                copiesNumOf_R = copiesNumOf_R / 2;
//                logger.info("中央协调器-开始处理R流内存过载,分区方案调整完成后系统的分区方案如下：");
//                logCurrentSystemConfiguration();
//            }
//
//            //处理S流中的内存过载
//            if (coordinatorStatus.isProcessSMemoryOver()) {
//                is_S_Processed = true;
//                copiesNumOf_S = copiesNumOf_S / 2;
//                logger.info("中央协调器-开始处理S流内存过载,分区方案调整完成后系统的分区方案如下：");
//                logCurrentSystemConfiguration();
//            }
//
//            //处理R流中的内存欠载
//            if (coordinatorStatus.isProcessRMemoryUnderLoad()) {
//                is_R_Processed = true;
//                copiesNumOf_R = copiesNumOf_R * 2;
//                logger.info("中央协调器-开始处理R流内存欠载,分区方案调整完成后系统的分区方案如下：");
//                logCurrentSystemConfiguration();
//            }
//
//            //处理S流中的内存欠载
//            if (coordinatorStatus.isProcessSMemoryUnderLoad()) {
//                is_S_Processed = true;
//                copiesNumOf_S = copiesNumOf_S * 2;
//                logger.info("中央协调器-开始处理S流内存欠载,分区方案调整完成后系统的分区方案如下：");
//                logCurrentSystemConfiguration();
//            }
//
//            //进行防止存储副本数量越界的处理，存储副本的数量不能小于1，且不能大于最大存储节点数量
//            copiesNumOf_R = Math.max(copiesNumOf_R, 1);
//            copiesNumOf_R = Math.min(copiesNumOf_R, storeNodeNumOf_R);
//            copiesNumOf_S = Math.max(copiesNumOf_S, 1);
//            copiesNumOf_S = Math.min(copiesNumOf_S, storeNodeNumOf_S);
//            logger.info("中央协调器:对R与S中的副本数量进行防越界处理（不小于1且不大于最大节点数量）,最终的分区方案如下：");
//            logCurrentSystemConfiguration();
//
//            //发起Joiner内存变更回合，
//            //-将最新的存储路由表发送给Router，之后等待所有Joiner调整完内存后，通知Router重新开始元组处理
//            uploadPartitionSchemeAndSyncJoinerMemoryChange();
//
//            logger.info("中央协调器-处理完成Joiner的内存变更！");
//
//            //让出锁一段时间，之后将进行过更改的流的状态归零
//            //此处等待一段时间是为了解决一些与同步有关的问题，在阻塞之后才可以重置状态
//            //此处若返回的是false，表示是由于超时导致的返回，因此可以结束内存变更处理
//            boolean await = wakeUpCoordinatorCondition.await(2000, TimeUnit.MILLISECONDS);
//
//            //将当前已经被更改过内存状态的流的状态重置
//            if (is_R_Processed) {
//                coordinatorStatus.setProcessRMemoryOver(false);
//                coordinatorStatus.setProcessRMemoryUnderLoad(false);
//                logger.info("中央协调器-R流中的状态被调整过！现在调整完成，R调整状态重置");
//            }
//            if (is_S_Processed) {
//                coordinatorStatus.setProcessSMemoryOver(false);
//                coordinatorStatus.setProcessSMemoryUnderLoad(false);
//                logger.info("中央协调器-S流中的状态被调整过！现在调整完成，S调整状态重置");
//            }
//
//            //若线程等待返回的是true，表示有内存变更事件唤醒线程
//            if (await) {
//                logger.info("中央协调器-在处理一次内存变更操作的过程中，又接收到了另一个流的内存变更请求，因此递归调用处理内存变更的方法。。。");
//                processJoinerChangeMemory(currentWakeUpEvent);
//            }
//
//        } catch (Exception e) {
//            e.printStackTrace();
//            System.err.println("中央协调器-处理Joiner节点的内存调整失败");
//        }finally {
//            lock.unlock();
//        }
//
//    }



    /**
     * 协调器关闭时执行
     */
    private void close() {
        synchronizer.close();
        logger.info("中央协调器关闭成功！");
    }

    /**
     * 初始化Zookeeper及其相关配置,创建所有系统需要用到的永久前缀节点
     */
    private void initZookeeper() {

        synchronizer = new ZookeeperBasedSynchronizer(GeneralParameters.CONNECT_STRING, GeneralParameters.SESSION_TIMEOUT);
        logger.info("中央协调器中，zookeeper同步器创建成功");

        creatSystemZookeeperPrefix();
        logger.info("中央协调器中，系统所需的Zookeeper前缀节点创建成功");
    }

    /**
     * 创建整个系统所需的所有前缀，均为永久节点
     * （在启动该中央协调器的Router中也应该创建前缀节点，此处有些多余，但是为保险，在此也加入相关代码）
     */
    private void creatSystemZookeeperPrefix() {
        synchronizer.creatZookeeperNodePersistent(CoModelParameters.ZOOKEEPER_PREFIX_OF_CO_MODEL, "CoModel".getBytes());
        synchronizer.creatZookeeperNodePersistent(CoModelParameters.ZOOKEEPER_PREFIX_OF_CO_MODEL_COORDINATOR, "Coordinator".getBytes());
        synchronizer.creatZookeeperNodePersistent(CoModelParameters.ZOOKEEPER_PREFIX_OF_CO_MODEL_ROUTER, "Router".getBytes());
        synchronizer.creatZookeeperNodePersistent(CoModelParameters.ZOOKEEPER_PREFIX_OF_CO_MODEL_JOINER, "Joiner".getBytes());
        synchronizer.creatZookeeperNodePersistent(CoModelParameters.ZOOKEEPER_PREFIX_OF_CO_MODEL_MONITOR, "Monitor".getBytes());
    }

    /**
     * 判断一个给定的节点编号是否是R流的存储节点
     * @param nodeIndex 节点编号
     * @return 如果是R流的存储节点，返回true
     */
    private boolean isNodeTheStoreNodeOf_R(int nodeIndex) {
        return nodeIndex < storeNodeNumOf_R;
    }



    /**
     * 用指定类型的事件唤醒主线程
     * @param event 当前发生的事件
     */
    private void wakeUpCoordinator(TriggerEvent event) {
        logger.info("中央协调器接收到事件-" + event.getEventType());
        lock.lock();
        try {
            currentWakeUpEvent = event;
            wakeUpCoordinatorCondition.signalAll();
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("中央协调器中，唤醒主线程失败！");
        }finally {
            lock.unlock();
        }
    }

    /**
     * 用于处理一般Joiner上传消息的监听器，在监听器中被调用，该方法的调用在同步块中
     * --在其中会根据接收到的Joiner上传消息不同进行分别处理
     * @param event 节点的事件
     * @param data 节点的内容
     */
    private void processCommonMessOfJoinerListener(WatchedEvent event, byte[] data) {

        TransferProtocolBasedZookeeper message = new TransferProtocolBasedZookeeper(data);
        int joinerIndex = ZookeeperBasedSynchronizer.getIndexOfMultiInstanceNodePath(event.getPath());
        logger.info("中央协调器-接收到Joiner-"+joinerIndex+"上传的信息");

        //处理Joiner内存过载消息
        if (message.isJoinerOverMemoryMess()) {
            logger.info("中央协调器-接收到Joiner-" + joinerIndex + "上传的内存过载消息。");
            joinerOverMemoryMessHandler(event, data, joinerIndex);
        }

    }

    /**
     * 处理Joiner上传的内存过载消息的处理器
     * @param joinerIndex 上传消息的Joiner编号
     */
    private void joinerOverMemoryMessHandler(WatchedEvent event, byte[] data, int joinerIndex) {
        //分别考虑R中和S中内存过载的情况
        if (isNodeTheStoreNodeOf_R(joinerIndex)) {  //R中存储节点内存过载
            //只有当系统同时不处于处理R流中的过载与欠载的状态下时，才会进行相关的处理
            if (!coordinatorStatus.isProcessRMemoryOver() && !coordinatorStatus.isProcessRMemoryUnderLoad()) {
                coordinatorStatus.setProcessRMemoryOver(true);
                logger.info("中央协调器-准备处理R流中的内存过载，触发该操作的Joiner编号为-" + joinerIndex);

                //将所有其他R的存储节点的状态置为过载，以防止其余节点再次发送过载消息
                TransferProtocolBasedZookeeper notifyMess = TransferProtocolBasedZookeeper.createJoinerOverMemoryMess("null");
                for (int i = 0; i < storeNodeNumOf_R; i++) {
                    synchronizer.setZookeeperNodeContent(
                            ZookeeperNodePathSetForCoModel.JOINER_RECEIVE_COORDINATOR_COMMON_PREFIX + i,
                            notifyMess.getTransferByteForThisMess());
                }
                logger.info("中央协调器-将所有R流中的存储节点的状态置为过载");

                //唤醒主线程
                wakeUpCoordinator(new TriggerEvent(WakeUpCoordinatorEventType.JOINER_MEMORY_CHANGE, event.getPath(), data));
            }
        } else {  //S中存储节点的消息
            //只有当系统同时不处于处理S流中的过载与欠载的状态下时，才会进行相关的处理
            if (!coordinatorStatus.isProcessSMemoryOver() && !coordinatorStatus.isProcessSMemoryUnderLoad()) {
                coordinatorStatus.setProcessSMemoryOver(true);
                logger.info("中央协调器-准备处理S流中的内存过载，触发该操作的Joiner编号为-" + joinerIndex);

                //将所有其他S的存储节点的状态置为过载，以防止其余节点再次发送过载消息
                TransferProtocolBasedZookeeper notifyMess = TransferProtocolBasedZookeeper.createJoinerOverMemoryMess("null");
                for (int i = storeNodeNumOf_R; i < totalJoinerNodeNum; i++) {
                    synchronizer.setZookeeperNodeContent(
                            ZookeeperNodePathSetForCoModel.JOINER_RECEIVE_COORDINATOR_COMMON_PREFIX + i,
                            notifyMess.getTransferByteForThisMess());
                }
                logger.info("中央协调器-将所有S流中的存储节点的状态置为过载");

                //唤醒主线程
                wakeUpCoordinator(new TriggerEvent(WakeUpCoordinatorEventType.JOINER_MEMORY_CHANGE, event.getPath(), data));

            }
        }
    }

    /**
     * 用于打印当前系统的配置信息
     */
    private void logCurrentSystemConfiguration() {
        logger.info("中央协调器:当前系统的配置为：R的存储节点数量为-" + storeNodeNumOf_R
                + "；R中每个元组保存的副本数量为-" + copiesNumOf_R
                + "；S的存储节点数量为-" + storeNodeNumOf_S
                + "；S中每个元组保存的副本数量为-" + copiesNumOf_S);
    }

    /**
     * 用于标识协调器的各种状态
     */
    static class CoordinatorStatus {
        //判断系统是否存活
        private boolean systemAlive = false;
        //判断系统是否正处于处理R中存储节点（或S中存储节点）中内存过载的过程中
        private boolean isProcessRMemoryOver = false;
        private boolean isProcessSMemoryOver = false;
        //判断系统是否正处于处理R中存储节点（或S中存储节点）中内存欠载的过程中
        private boolean isProcessRMemoryUnderLoad = false;
        private boolean isProcessSMemoryUnderLoad = false;

        public boolean isProcessRMemoryUnderLoad() {
            return isProcessRMemoryUnderLoad;
        }

        public void setProcessRMemoryUnderLoad(boolean processRMemoryUnderLoad) {
            isProcessRMemoryUnderLoad = processRMemoryUnderLoad;
        }

        public boolean isProcessSMemoryUnderLoad() {
            return isProcessSMemoryUnderLoad;
        }

        public void setProcessSMemoryUnderLoad(boolean processSMemoryUnderLoad) {
            isProcessSMemoryUnderLoad = processSMemoryUnderLoad;
        }

        boolean isProcessRMemoryOver() {
            return isProcessRMemoryOver;
        }

        void setProcessRMemoryOver(boolean processRMemoryOver) {
            isProcessRMemoryOver = processRMemoryOver;
        }

        boolean isProcessSMemoryOver() {
            return isProcessSMemoryOver;
        }

        void setProcessSMemoryOver(boolean processSMemoryOver) {
            isProcessSMemoryOver = processSMemoryOver;
        }

        boolean isSystemAlive() {
            return systemAlive;
        }

        void setSystemAlive(boolean systemAlive) {
            this.systemAlive = systemAlive;
        }
    }

    /**
     * 定义当前发生的事件，其中包括发生的事件类型以及触发事件的节点
     */
    static class TriggerEvent {
        //当前的事件类型
        private WakeUpCoordinatorEventType eventType;
        //触发事件的节点
        private String triggerNodePath;
        //触发事件节点的内容
        private byte[] nodeContent;

        TriggerEvent(WakeUpCoordinatorEventType eventType, String triggerNodePath, byte[] nodeContent) {
            this.eventType = eventType;
            this.triggerNodePath = triggerNodePath;
            this.nodeContent = nodeContent;
        }

        public byte[] getNodeContent() {
            return nodeContent;
        }

        public void setNodeContent(byte[] nodeContent) {
            this.nodeContent = nodeContent;
        }

        WakeUpCoordinatorEventType getEventType() {
            return eventType;
        }

        public void setEventType(WakeUpCoordinatorEventType eventType) {
            this.eventType = eventType;
        }

        public String getTriggerNodePath() {
            return triggerNodePath;
        }

        public void setTriggerNodePath(String triggerNodePath) {
            this.triggerNodePath = triggerNodePath;
        }
    }

    /**
     * 用于定义唤醒协调器的各种事件类型
     */
    enum WakeUpCoordinatorEventType {
        DEAD, JOINER_MEMORY_CHANGE;
    }
}
