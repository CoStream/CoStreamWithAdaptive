package coModel.tools.sync;

import org.apache.zookeeper.WatchedEvent;

/**
 * 不带返回值的同步监视器，当监听的事件被触发时，该方法会被调用
 */
@FunctionalInterface
public interface SyncListenerWithoutReturn {
    /**
     * 当监听的事件被触发时，该方法会被调用
     */
    public void onTrigger(WatchedEvent event);

}
