package coModel.tools.sync;

import org.apache.log4j.Logger;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class ZookeeperBasedSynchronizer implements Serializable {

    //用于序列化，后加的，序列化也是后加的
    private static final long serialVersionUID = 1044404003L;

    //需要用到的zookeeper客户端
    private ZooKeeper zkClient;

    //获取logger对象用于日志记录
    private static final Logger logger = Logger.getLogger(ZookeeperBasedSynchronizer.class.getName());

    /**
     * 构造器，在其中初始化zookeeper客户端
     * @param zookeeperAddressString zookeeper地址
     * @param zookeeperSessionTimeout zookeeper超时时间
     */
    public ZookeeperBasedSynchronizer(String zookeeperAddressString, int zookeeperSessionTimeout) {
        //初始化zookeeper
        try {
            zkClient = new ZooKeeper(zookeeperAddressString, zookeeperSessionTimeout, null);
        } catch (IOException e) {
            e.printStackTrace();
            System.err.println("异常信息 ：Zookeeper同步器 中 zookeeper 初始化连接失败");
        }
    }

    /**
     * 若一个节点不存在则创建该节点，否则，不执行任何操作
     * --创建的节点是永久节点
     * @param nodePath 节点路径
     * @param nodeContent 节点内容
     * @return 若节点已存在，或者节点创建成功，则返回true；若节点不存在且创建失败，返回false
     */
    public boolean creatZookeeperNodePersistent(String nodePath, byte[] nodeContent) {

        boolean isNodeExists = false;

        try {
            Stat exists = zkClient.exists(nodePath, false);
            if (exists == null) {
                zkClient.create(nodePath, nodeContent, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                isNodeExists = true;
            } else {
                isNodeExists = true;
            }
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
            System.err.println("异常信息 ：Zookeeper同步器 中创建永久节点" + nodePath + "失败！");
        }

        return isNodeExists;
    }

    /**
     * 若一个节点不存在则创建该节点，否则，不执行任何操作
     * --创建的节点是临时节点,临时节点不能有子节点
     * @param nodePath 节点路径
     * @param nodeContent 节点内容
     * @return 若节点已存在，或者节点创建成功，则返回true；若节点不存在且创建失败，返回false
     */
    public boolean creatZookeeperNodeEphemeral(String nodePath, byte[] nodeContent) {

        boolean isNodeExists = false;

        try {
            Stat exists = zkClient.exists(nodePath, false);
            if (exists == null) {
                zkClient.create(nodePath, nodeContent, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
                isNodeExists = true;
            } else {
                isNodeExists = true;
            }
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
            System.err.println("异常信息 ：Zookeeper同步器 中创建临时节点" + nodePath + "失败！");
        }

        return isNodeExists;
    }

    /**
     * 改变一个指定节点的内容，若节点不存在，则创建节点(临时节点)并设置内容
     * @param nodePath 节点路径
     * @param nodeContent 节点内容
     * @return 若节点内容改变成功，则返回true
     */
    public boolean setZookeeperNodeContent(String nodePath, byte[] nodeContent) {

        boolean isSetSuccess = false;

        try {
            Stat exists = zkClient.exists(nodePath, false);
            if (exists == null) {
                zkClient.create(nodePath, nodeContent, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
                isSetSuccess = true;
            } else {
                zkClient.setData(nodePath, nodeContent, exists.getVersion());
                isSetSuccess = true;
            }
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
            System.err.println("异常信息 ：Zookeeper同步器 中更新节点" + nodePath + "失败！");
        }

        return isSetSuccess;
    }

    /**
     * 根据给定的前缀和实例数量，将指定的内容上传到所有这些实例的节点中，节点编号从 0 到 instancesNum-1
     * @param nodePathPrefix 所有实例的前缀
     * @param instancesNum 实例的数量
     * @param nodeContent 要上传的内容，所有的节点的内容都相同
     */
    public void setAllMultipleInstancesNodeContent(String nodePathPrefix, int instancesNum, byte[] nodeContent) {
        for (int i = 0; i < instancesNum; i++) {
            setZookeeperNodeContent(nodePathPrefix + i, nodeContent);
        }
    }

    /**
     * 返回指定节点的内容
     * @param nodePath 要获取内容的节点
     * @return 对应节点的内容
     */
    public byte[] getZookeeperNodeContent(String nodePath) {
        byte[] data = null;
        try {
            data = zkClient.getData(nodePath, false, null);
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
            System.err.println("异常信息 ：Zookeeper同步器 中获取节点" + nodePath + "的内容失败！");
        }
        return data;
    }

    /**
     * 判断给定路径的节点是否存在
     * @param nodePath 节点路径
     * @return 如果节点存在，则返回 true
     */
    public boolean isNodeExists(String nodePath) {
        Stat exists = null;
        try {
            exists = zkClient.exists(nodePath, false);
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
            System.err.println("异常信息 ：Zookeeper同步器 中判断节点" + nodePath + "是否存在的操作失败！");
        }

        return exists != null;
    }

    /**
     * 单次监听对应的节点，每当对应的节点被改变时，给定的方法会被触发；
     * --方法只会被触发一次
     * @param nodePath 要监视的节点路径
     * @param listener 在事件被触发时要执行的方法，该方法没有返回值
     * @return 若设定监视器失败，则返回false
     */
    public boolean creatOnceListenerWithoutReturn(String nodePath, SyncListenerWithoutReturn listener) {
        try {
            zkClient.exists(nodePath, new OnceWatcherForSyncListenerWithoutReturn(listener));
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
            System.err.println("异常信息 ：Zookeeper同步器 中单次监听节点" + nodePath + "创建失败！");
            return false;
        }
        return true;
    }

    /**
     * 单次监听对应的节点，每当对应的节点被改变时，给定的方法会被触发；触发的方法中会传入对应节点的内容
     * --方法只会被触发一次
     * @param nodePath 要监视的节点路径
     * @param listener 在事件被触发时要执行的方法，该方法中会传入节点的内容，并且没有返回值
     * @return 若设定监视器失败，则返回false
     */
    public boolean creatOnceRichListenerWithoutReturn(String nodePath, RichListenerWithoutReturn listener) {
        try {
            zkClient.exists(nodePath, new OnceWatcherForRichSyncListenerWithoutReturn(listener));
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
            System.err.println("异常信息 ：Zookeeper同步器 中单次监听节点" + nodePath + "创建失败！");
            return false;
        }
        return true;
    }

    /**
     * 循环监听对应的节点，每当对应的节点被改变时，给定的方法会被触发；
     * --在方法被触发之后会再次监听对应的节点，以此实现循环监听
     * @param nodePath 要监视的节点路径
     * @param listener 在事件被触发时要执行的方法，该方法没有返回值
     * @return 若设定监视器失败，则返回false
     */
    public boolean creatLoopListenerWithoutReturn(String nodePath, SyncListenerWithoutReturn listener) {
        try {
            zkClient.exists(nodePath, new LoopWatcherForSyncListenerWithoutReturn(zkClient, nodePath, listener));
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
            System.err.println("异常信息 ：Zookeeper同步器 中循环监听节点" + nodePath + "创建失败！");
            return false;
        }
        return true;
    }

    /**
     * 循环监听对应的节点，每当对应的节点被改变时，给定的方法会被触发；同时会向方法中传入节点的最新内容
     * --在方法被触发之后会再次监听对应的节点，以此实现循环监听
     * @param nodePath 要监视的节点路径
     * @param listener 在事件被触发时要执行的方法，该方法没有返回值
     * @return 若设定监视器失败，则返回false
     */
    public boolean creatLoopRichListenerWithoutReturn(String nodePath, RichListenerWithoutReturn listener) {
        try {
            zkClient.exists(nodePath, new LoopWatcherForRichSyncListenerWithoutReturn(listener));
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
            System.err.println("异常信息 ：Zookeeper同步器 中循环监听节点" + nodePath + "创建失败！");
            return false;
        }
        return true;
    }

    /**
     * 创建一个一般的同步回合
     * @param respondNodePathList 所有要监听的节点
     * @param notifyNodePath 通知开启回合的节点
     * @return 对应的一般同步回合
     */
    private InterComponentSyncRound creatNormalSyncRound(List<String> respondNodePathList, String notifyNodePath) {
        return new InterComponentSyncRound(zkClient, respondNodePathList, notifyNodePath);
    }


    /**
     * 创建一个一对一的同步回合，监视节点和通知节点都是一个
     * @param respondNodePath 监视节点
     * @param notifyNodePath 通知节点
     * @return 对应的一对一同步回合
     */
    private InterComponentSyncRound creatOneToOneSyncRound(String respondNodePath, String notifyNodePath) {
        LinkedList<String> respondNodePathList = new LinkedList<>();
        respondNodePathList.add(respondNodePath);
        return new InterComponentSyncRound(zkClient, respondNodePathList, notifyNodePath);
    }


    /**
     * 根据给定的回合对象，返回一个-一对多-的同步回合的发起者
     * @param syncRound 指定的回合
     * @param responderNum 对应回合中需要进行响应的节点数量
     * @return 一对多-回合的发起者
     */
    private InterComponentSyncRound getOneToALLManySyncRoundInitiators(SyncRound syncRound, int responderNum) {
        ArrayList<String> respondNodeList = new ArrayList<>();
        for (int i = 0; i < responderNum; i++) {
            respondNodeList.add(syncRound.getRespondNodePathOrPrefix() + i);
        }
        return creatNormalSyncRound(respondNodeList, syncRound.getNotifyNodePath());
    }

    /**
     * 根据给定的回合对象，返回一个-一对一-的同步回合的发起者
     * @param syncRound 指定的回合
     * @return 一对一-回合的发起者
     */
    private InterComponentSyncRound getOneToOneSyncRoundInitiators(SyncRound syncRound) {
        return creatOneToOneSyncRound(syncRound.getRespondNodePathOrPrefix(), syncRound.getNotifyNodePath());
    }

    /**
     * 发起一个-一对多-的回合，并等待回合同步结束（调用的线程会阻塞在这里直到同步结束）
     * --一个回合的使用方法如下：
     * --（1）节点A调用 listeningForOneToAllManySyncRoundOnLoop 监听回合的发起
     * --（2）节点B调用 initiateOneToAllManySyncRoundAndSyncWait 发起回合并等待
     * --（3）节点A进行相应的处理
     * --（4）节点C调用 respondOneToAllManySyncRound 通知B回合结束
     * @param syncRound 要发起的回合
     * @param responderNum 对应回合中需要进行响应的节点数量
     * @param notifyMessage 要写入通知节点的内容
     */
    public void initiateOneToAllManySyncRoundAndSyncWait(SyncRound syncRound, int responderNum, byte[] notifyMessage) {
        InterComponentSyncRound oneToALLManySyncRoundInitiators = getOneToALLManySyncRoundInitiators(syncRound, responderNum);
        oneToALLManySyncRoundInitiators.startSync(notifyMessage);
        oneToALLManySyncRoundInitiators.waitForSyncComplete();
    }

    /**
     * 发起一个-一对一-的回合，并等待回合同步结束（调用的线程会阻塞在这里直到同步结束）
     * @param syncRound 要发起的回合
     * @param notifyMessage 要写入通知节点的内容
     */
    public void initiateOneToOneSyncRoundAndSyncWait(SyncRound syncRound, byte[] notifyMessage) {
        InterComponentSyncRound oneToOneSyncRoundInitiators = getOneToOneSyncRoundInitiators(syncRound);
        oneToOneSyncRoundInitiators.startSync(notifyMessage);
        oneToOneSyncRoundInitiators.waitForSyncComplete();
    }

    /**
     * 循环监听一个-一对多-回合的发起，当回合发起时，会执行给定的方法，之后重复进行监听
     * @param syncRound 要监听的回合
     * @param listener 回合开始时需要执行的动作，可多次执行
     */
    public void listeningForOneToAllManySyncRoundOnLoop(SyncRound syncRound, SyncListenerWithoutReturn listener) {
        creatLoopListenerWithoutReturn(syncRound.getNotifyNodePath(), listener);
    }


    /**
     * 循环监听一个-一对多-回合的发起，当回合发起时，会执行给定的方法，之后重复进行监听。在方法中会传入节点内容
     * @param syncRound 要监听的回合
     * @param listener 回合开始时需要执行的动作，可多次执行
     */
    public void listeningRichForOneToAllManySyncRoundOnLoop(SyncRound syncRound, RichListenerWithoutReturn listener) {
        creatLoopRichListenerWithoutReturn(syncRound.getNotifyNodePath(), listener);
    }

    /**
     * 单次监听一个-一对多-回合的发起，当回合发起时，会执行给定的方法，对应的方法只会触发一次
     * @param syncRound 要监听的回合
     * @param listener 回合开始时需要执行的动作，执行一次
     */
    public void listeningForOneToAllManySyncRoundOnce(SyncRound syncRound, SyncListenerWithoutReturn listener) {
        creatOnceListenerWithoutReturn(syncRound.getNotifyNodePath(), listener);
    }

    /**
     * 循环监听一个-一对一-回合的发起，当回合发起时，会执行给定的方法，之后重复进行监听（实现与一对多回合的一样）
     * @param syncRound 要监听的回合
     * @param listener 回合开始时需要执行的动作，可多次执行
     */
    public void listeningForOneToOneSyncRoundOnLoop(SyncRound syncRound, SyncListenerWithoutReturn listener) {
        listeningForOneToAllManySyncRoundOnLoop(syncRound, listener);
    }

    /**
     * 单次监听一个-一对一-回合的发起，当回合发起时，会执行给定的方法，方法只会被执行一次（实现与一对多回合的一样）
     * @param syncRound 要监听的回合
     * @param listener 回合开始时需要执行的动作，执行一次
     */
    public void listeningForOneToOneSyncRoundOnce(SyncRound syncRound, SyncListenerWithoutReturn listener) {
        listeningForOneToAllManySyncRoundOnce(syncRound, listener);
    }

    /**
     * 响应一个-一对多-回合
     * @param syncRound 对应回合
     * @param responderNum 响应者的编号
     * @param content 要写入响应节点中的内容
     * @return 若响应成功，则返回true
     */
    public boolean respondOneToAllManySyncRound(SyncRound syncRound, int responderNum, byte[] content) {
        return setZookeeperNodeContent(syncRound.getRespondNodePathOrPrefix() + responderNum, content);
    }

    /**
     * 响应一个-一对一-回合
     * @param syncRound 对应回合
     * @param content 要写入响应节点中的内容
     * @return 若响应成功，则返回true
     */
    public boolean respondOneToOneSyncRound(SyncRound syncRound, byte[] content) {
        return setZookeeperNodeContent(syncRound.getRespondNodePathOrPrefix(), content);
    }

    /**
     * 关闭该同步器
     * @return 成功关闭则返回true。
     */
    public boolean close() {
        try {
            zkClient.close();
        } catch (InterruptedException e) {
            e.printStackTrace();
            System.err.println("异常信息 ：Zookeeper同步器关闭失败！");
            return false;
        }
        return true;
    }

    /**
     * 输入一个具有多个实例的节点全路径字符串（如多个Joiner的实例），返回对应路径的编号
     * 原理是每个多实例中的单个节点路径由 前缀-编号 构成，用’-‘切分字符串即可获得编号
     * @param path 要获取编号的节点路径
     * @return 若输入的路径不是标准的多节点路径，则返回-1，否则返回对应的编号
     */
    public static int getIndexOfMultiInstanceNodePath(String path) {
        String[] split = path.split("-");
        if (split.length < 2) {
            return -1;
        }
        return Integer.parseInt(split[split.length - 1]);
    }


    /**
     * 循环监听指定的节点。
     * --当事件触发时执行对应的方法，之后再次监听该节点以实现循环监听
     */
    static class LoopWatcherForSyncListenerWithoutReturn implements Watcher {

        private ZooKeeper zkClient;
        private String nodePath;
        private SyncListenerWithoutReturn listener;


        public LoopWatcherForSyncListenerWithoutReturn(final ZooKeeper zkClient, String nodePath, SyncListenerWithoutReturn listener) {
            this.zkClient = zkClient;
            this.nodePath = nodePath;
            this.listener = listener;
        }

        @Override
        public void process(WatchedEvent event) {
            //执行对应方法
            listener.onTrigger(event);

            //循环监听
            try {
                zkClient.exists(nodePath, new LoopWatcherForSyncListenerWithoutReturn(zkClient, nodePath, listener));
            } catch (KeeperException | InterruptedException e) {
                e.printStackTrace();
                System.err.println("异常信息 ：Zookeeper同步器 中 LoopWatcherForSyncListenerWithoutReturn 循环监听节点" + nodePath + "失败！");
            }
        }
    }

    /**
     * 循环监听指定的节点。并向调用方法中传入节点内容
     * --当事件触发时执行对应的方法，之后再次监听该节点以实现循环监听
     */
    class LoopWatcherForRichSyncListenerWithoutReturn implements Watcher {

        private RichListenerWithoutReturn listener;


        public LoopWatcherForRichSyncListenerWithoutReturn(RichListenerWithoutReturn listener) {
            this.listener = listener;
        }

        @Override
        public void process(WatchedEvent event) {

            byte[] zookeeperNodeContent = getZookeeperNodeContent(event.getPath());
            //执行对应方法
            listener.onTrigger(event, zookeeperNodeContent);

            //循环监听
            try {
                zkClient.exists(event.getPath(), new LoopWatcherForRichSyncListenerWithoutReturn(listener));
            } catch (KeeperException | InterruptedException e) {
                e.printStackTrace();
                System.err.println("异常信息 ：Zookeeper同步器 中 LoopWatcherForRichSyncListenerWithoutReturn 循环监听节点" + event.getPath() + "失败！");
            }
        }
    }

    /**
     * 单次节点监听。
     * --当事件发生时，执行指定方法，该方法只被执行一次
     */
    static class OnceWatcherForSyncListenerWithoutReturn implements Watcher {

        private SyncListenerWithoutReturn listener;

        public OnceWatcherForSyncListenerWithoutReturn(SyncListenerWithoutReturn listener) {
            this.listener = listener;
        }

        @Override
        public void process(WatchedEvent event) {
            //执行对应方法
            listener.onTrigger(event);
        }
    }

    /**
     * 单次节点监听。同时会获取该节点中的内容
     * --当事件发生时，执行指定方法，该方法只被执行一次
     */
    class OnceWatcherForRichSyncListenerWithoutReturn implements Watcher {

        private RichListenerWithoutReturn listener;

        public OnceWatcherForRichSyncListenerWithoutReturn(RichListenerWithoutReturn listener) {
            this.listener = listener;
        }

        @Override
        public void process(WatchedEvent event) {
            byte[] zookeeperNodeContent = getZookeeperNodeContent(event.getPath());
            //执行对应方法
            listener.onTrigger(event, zookeeperNodeContent);
        }
    }



}

