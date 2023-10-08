package coModel.tools.sync;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * 基于Zookeeper实现系统不同组件（Router，Joiner，协调器）之间的同步工具
 * --每次该对象的完整运行代表一次同步回合操作
 * 使用方法：
 * （1）创建对象object
 * （2）调用 object.startSync 开始进行回合同步
 * （3）调用 object.waitForSyncComplete 同步等待回合结束，同步成功
 * --也可调用isSyncComplete自行判断是否同步成功
 */
public class InterComponentSyncRound implements Serializable {

    //用于序列化，后加的，序列化也是后加的
    private static final long serialVersionUID = 1044404001L;

    //需要用到的zookeeper客户端，由外界传入
    private ZooKeeper zkClient;
    //要监视的节点列表（响应节点）
    private List<String> monitorNodePathList;
    //要发送通知消息的节点(通知节点)
    private String notifyNodePath;

    //用于进行同步的标记位列表，其中每一个元素对应一个要进行同步的目标节点
    //--其中的每一个元素初始为false，表示未接受到对应的节点的信息；当接收到一个节点的消息后，对应位置为true
    private ArrayList<Boolean> flagList;

    //标记当前同步器是否正处于一次同步中，若上一次的同步未完成，则无法开启新的同步;初始为false表示未处于同步中
    private boolean isSyncInProgress = false;

    public InterComponentSyncRound(ZooKeeper zkClient, List<String> respondNodePathList, String notifyNodePath) {
        this.zkClient = zkClient;
        this.monitorNodePathList = respondNodePathList;
        this.notifyNodePath = notifyNodePath;
    }

    /**
     * 开始进行同步，向信号节点发送指定消息开启同步，并准备接收目标节点的同步消息
     * @param notifyMessage 向通知节点发送的消息
     * @return 若该方法正确执行，则返回true；
     */
    public boolean startSync(byte[] notifyMessage) {
        //如果当前正处于一次同步中，则无法再次开启同步
        if (isSyncInProgress) {
            return false;
        }

        //表示当前正在进行一次同步
        isSyncInProgress = true;
        //开始监视所有目标节点
        flagList = new ArrayList<>();
        for (int i = 0; i < monitorNodePathList.size(); i++) {
            flagList.add(false);
            try {
                zkClient.exists(monitorNodePathList.get(i), new FlagListWatch(i));
            } catch (KeeperException | InterruptedException e) {
                e.printStackTrace();
                System.err.println("异常信息：InterComponentZookeeperBasedSyncTool 中创建监视节点失败！");
            }
        }

        //更改信号节点的信息，以通知同步开始
        try {
            Stat exists = zkClient.exists(notifyNodePath, false);
            if (exists == null){
                zkClient.create(notifyNodePath,notifyMessage, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            }else {
                zkClient.setData(notifyNodePath,notifyMessage,exists.getVersion());
            }
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
            System.err.println("异常信息：InterComponentZookeeperBasedSyncTool 中设置信号节点信息失败！");
        }

        return true;
    }

    /**
     * 判断同步是否完成
     * @return 若同步完成，则返回true；
     */
    public boolean isSyncComplete() {

        if (flagList == null) {
            return true;
        }

        boolean isCompleted = true;
        for (boolean flag : flagList) {
            if (!flag) {
                isCompleted = false;
                break;
            }
        }

        if (isCompleted) {
            isSyncInProgress = false;
        }
        return isCompleted;
    }

    /**
     * 调用该方法的线程会一直阻塞，直到上一次同步完成
     */
    public void waitForSyncComplete() {
        while (!isSyncComplete()) {
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
                System.err.println("异常信息：InterComponentZookeeperBasedSyncTool 中同步等待同步操作成功方法失败！");
            }
        }
    }

    /**
     * 用于监视指定的节点，当指定的节点的数据被更改时会被触发
     */
     class FlagListWatch implements Watcher {
        //监视的节点在标记列表中对应的位置
        private int monitorPosition;

        public FlagListWatch(int monitorPosition) {
            this.monitorPosition = monitorPosition;
        }

        @Override
        public void process(WatchedEvent event) {
            flagList.set(monitorPosition, true);
        }
    }

}
