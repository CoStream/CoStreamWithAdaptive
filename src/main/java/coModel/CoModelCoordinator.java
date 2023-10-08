package coModel;

import coModel.tools.sync.*;
import common.GeneralParameters;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.log4j.Logger;
import org.apache.zookeeper.WatchedEvent;
import tools.common.ParseExperimentParametersTool;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * CoModel模型的中央协调器，负责Router与Joiner之间的同步协调等操作
 */
public class CoModelCoordinator extends Thread implements Serializable {

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
    private static final Logger logger = Logger.getLogger(CoModelCoordinator.class.getName());

    @Override
    public void run() {
        //协调器启动时调用一次
        open();

        //每次协调器被唤醒，且系统存活，则均会执行一次process方法
        lock.lock();
        try {
            while (coordinatorStatus.isSystemAlive()) {
                wakeUpCoordinatorCondition.await();
                if (!coordinatorStatus.isSystemAlive()) {
                    break;
                }
                logger.info("中央协调器-主线程被唤醒！开始处理事件");
                //处理当前事件
                process(currentWakeUpEvent);
                logger.info("中央协调器-主线程休眠！等待下一次事件的触发");
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

        System.out.println("中央协调器启动成功！");
        logger.info("中央协调器启动成功！");
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

        //范围连接的范围
        double R_surpass_S = Double.parseDouble(
                ParseExperimentParametersTool.parseExpParametersForNameWithinByteArray(argsBytes,
                        ParseExperimentParametersTool.PARAM_NAME_OF_R_SURPASS_S));
        double R_behind_S = Double.parseDouble(
                ParseExperimentParametersTool.parseExpParametersForNameWithinByteArray(argsBytes,
                        ParseExperimentParametersTool.PARAM_NAME_OF_R_BEHIND_S));

        //获取当前实验程序的配置字符串
        String expArgsString = new String(argsBytes);

        //打印系统当前的配置信息
        logger.info("中央协调器:分区方案初始化成功！");
        logger.info("中央协调器:当前系统中每个Joiner设定的剩余内存下限为(MB)-" + CoModelParameters.SYSTEM_MAX_REMAIN_MEMORY);
        logger.info("中央协调器:程序的上下游算子之间元组刷新时间(ms)为：" + GeneralParameters.FLINK_OUTPUT_FLUSH_PERIOD);
        logger.info("中央协调器:每个Joiner实例中子窗口的时间跨度为（s）：" + (CoModelParameters.SUB_INDEX_TIME_INTERVAL / 1000));

        logCurrentSystemConfiguration();

        //在标准输出中打印系统相关配置，为的是显示的更加明显
        System.out.println("中央协调器:当前系统的配置为：R的存储节点数量为-" + storeNodeNumOf_R
                + "；R中每个元组保存的副本数量为-" + copiesNumOf_R
                + "；S的存储节点数量为-" + storeNodeNumOf_S
                + "；S中每个元组保存的副本数量为-" + copiesNumOf_S);
        System.out.println("中央协调器:当前系统中每个Joiner设定的剩余内存下限为(MB)-" + CoModelParameters.SYSTEM_MAX_REMAIN_MEMORY);
        System.out.println("中央协调器:程序的上下游算子之间元组刷新时间(ms)为：" + GeneralParameters.FLINK_OUTPUT_FLUSH_PERIOD);
        System.out.println("中央协调器:每个Joiner实例中子窗口的时间跨度为（s）：" + (CoModelParameters.SUB_INDEX_TIME_INTERVAL / 1000));
        System.out.println("中央协调器:范围连接的范围为：R_surpass_S-" + R_surpass_S + ";R_behind_S-" + R_behind_S);
        System.out.println("中央协调器:当前程序的输入配置为：" + expArgsString);
        System.out.println("中央协调器:每个Slot配置的最大存储元组容量（M）：" + (CoModelParameters.MAX_BYTE_CONSUME_IN_EACH_JOINER / 1024 / 1024));
        System.out.println("中央协调器：当前实验的窗口总大小(所有子窗口的和，单位：秒)为" + GeneralParameters.WINDOW_SIZE);

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
     * 协调器启动后实际的执行逻辑
     * 每当有事件被触发时，该方法便会被调用一次
     * @param  currentWakeUpEvent 当前发生的事件
     */
    private void process(TriggerEvent currentWakeUpEvent) {
        //根据不同的事件类型进行相应的处理
        switch (currentWakeUpEvent.getEventType()) {
            case JOINER_MEMORY_CHANGE:  //处理Joiner内存变更事件（包括内存过载与欠载）
                processJoinerChangeMemory(currentWakeUpEvent);
                break;
            case DEAD:
                break;
        }

        logger.info("中央协调器-处理一次事件彻底完成！");

    }

    /**
     * 处理Joiner内存变更事件（包括内存过载与欠载）
     * @param currentWakeUpEvent 当前的事件
     */
    private void processJoinerChangeMemory(TriggerEvent currentWakeUpEvent) {
        logger.info("中央协调器-开始处理Joiner节点的内存变更事件");
        lock.lock();
        try {
            //等待一段时间，等待两个流的Joiner都进行完内存判断，尽量令两个流在同一个同步周期中改变内存
            //该方法会释放锁，并在一段时间之后自动被执行
            mainTreadSleepCondition.await(5000, TimeUnit.MILLISECONDS);

            //用于指示R或者S的内存是否被处理过，用于解决一些与同步有关的问题
            boolean is_R_Processed = false;
            boolean is_S_Processed = false;

            //处理R流中的内存过载
            //TODO 在此需要考虑副本数为0的情况，现在还没考虑到
            if (coordinatorStatus.isProcessRMemoryOver()) {
                is_R_Processed = true;
                copiesNumOf_R = copiesNumOf_R / 2;
                logger.info("中央协调器-开始处理R流内存过载,分区方案调整完成后系统的分区方案如下：");
                logCurrentSystemConfiguration();
            }

            //处理S流中的内存过载
            if (coordinatorStatus.isProcessSMemoryOver()) {
                is_S_Processed = true;
                copiesNumOf_S = copiesNumOf_S / 2;
                logger.info("中央协调器-开始处理S流内存过载,分区方案调整完成后系统的分区方案如下：");
                logCurrentSystemConfiguration();
            }

            //处理R流中的内存欠载
            if (coordinatorStatus.isProcessRMemoryUnderLoad()) {
                is_R_Processed = true;
                copiesNumOf_R = copiesNumOf_R * 2;
                logger.info("中央协调器-开始处理R流内存欠载,分区方案调整完成后系统的分区方案如下：");
                logCurrentSystemConfiguration();
            }

            //处理S流中的内存欠载
            if (coordinatorStatus.isProcessSMemoryUnderLoad()) {
                is_S_Processed = true;
                copiesNumOf_S = copiesNumOf_S * 2;
                logger.info("中央协调器-开始处理S流内存欠载,分区方案调整完成后系统的分区方案如下：");
                logCurrentSystemConfiguration();
            }

            //进行防止存储副本数量越界的处理，存储副本的数量不能小于1，且不能大于最大存储节点数量
            copiesNumOf_R = Math.max(copiesNumOf_R, 1);
            copiesNumOf_R = Math.min(copiesNumOf_R, storeNodeNumOf_R);
            copiesNumOf_S = Math.max(copiesNumOf_S, 1);
            copiesNumOf_S = Math.min(copiesNumOf_S, storeNodeNumOf_S);
            logger.info("中央协调器:对R与S中的副本数量进行防越界处理（不小于1且不大于最大节点数量）,最终的分区方案如下：");
            logCurrentSystemConfiguration();

            //发起Joiner内存变更回合，
            //-将最新的存储路由表发送给Router，之后等待所有Joiner调整完内存后，通知Router重新开始元组处理
            uploadPartitionSchemeAndSyncJoinerMemoryChange();

            logger.info("中央协调器-处理完成Joiner的内存变更！");

            //让出锁一段时间，之后将进行过更改的流的状态归零
            //此处等待一段时间是为了解决一些与同步有关的问题，在阻塞之后才可以重置状态
            //此处若返回的是false，表示是由于超时导致的返回，因此可以结束内存变更处理
            boolean await = wakeUpCoordinatorCondition.await(2000, TimeUnit.MILLISECONDS);

            //将当前已经被更改过内存状态的流的状态重置
            if (is_R_Processed) {
                coordinatorStatus.setProcessRMemoryOver(false);
                coordinatorStatus.setProcessRMemoryUnderLoad(false);
                logger.info("中央协调器-R流中的状态被调整过！现在调整完成，R调整状态重置");
            }
            if (is_S_Processed) {
                coordinatorStatus.setProcessSMemoryOver(false);
                coordinatorStatus.setProcessSMemoryUnderLoad(false);
                logger.info("中央协调器-S流中的状态被调整过！现在调整完成，S调整状态重置");
            }

            //若线程等待返回的是true，表示有内存变更事件唤醒线程
            if (await) {
                logger.info("中央协调器-在处理一次内存变更操作的过程中，又接收到了另一个流的内存变更请求，因此递归调用处理内存变更的方法。。。");
                processJoinerChangeMemory(currentWakeUpEvent);
            }

        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("中央协调器-处理Joiner节点的内存调整失败");
        }finally {
            lock.unlock();
        }

    }

    /**
     * 发起Joiner内存变更回合，包括：
     * （1）上传最新的分区方案给Joiner和Router
     * （2）通知Router开始进行Joiner对齐消息的发送
     * （3）Joiner对齐由Joiner发送的对齐消息
     * （4）Joiner变更内部的存储结构（减少内存/增加内存）
     * （5）Joiner通知协调器变更内存完成
     * （6）协调器通知Router内存变更完成，Router可以正确处理数据
     */
    private void uploadPartitionSchemeAndSyncJoinerMemoryChange() {
        //最新的分区方案
        Tuple4<Integer, Integer, Integer, Integer> partitionScheme
                = new Tuple4<>(storeNodeNumOf_R, copiesNumOf_R, storeNodeNumOf_S, copiesNumOf_S);
        logger.info("中央协调器-开始上传最新的分区方案，并开启Joiner内存变更回合，上传的分区方案为：" + partitionScheme);

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
