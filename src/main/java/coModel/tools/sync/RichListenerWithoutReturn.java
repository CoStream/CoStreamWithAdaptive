package coModel.tools.sync;

import org.apache.zookeeper.WatchedEvent;

/**
 * 不带返回值的同步监视器，当监听的事件被触发时，该方法会被调用，同时会获取对应节点的内容
 */
public interface RichListenerWithoutReturn {
    /**
     * 当监听的事件被触发时，该方法会被调用
     * @param event 节点发生的事件类型
     * @param data 节点的内容
     */
    public void onTrigger(WatchedEvent event, byte[] data);

}
