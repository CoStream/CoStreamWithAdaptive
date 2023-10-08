package coModel.tools.sync;

import java.io.Serializable;

public class SyncRound implements Serializable {
    //用于序列化，后加的，序列化也是后加的
    private static final long serialVersionUID = 1044404002L;

    //用于开启一个回合的通知节点
    private String notifyNodePath;
    //用于响应一个回合的响应节点。
    //--若需要进行响应的是一个组件实体，则该路径指的是一个节点的路径；
    //--若是需要进行响应的是多个组件实体，则该路径指的是每个组件响应节点路径的前缀，其后面直接接对应组件实例的编号。
    private String respondNodePathOrPrefix;

    public SyncRound(String notifyNodePath, String respondNodePathOrPrefix) {
        this.notifyNodePath = notifyNodePath;
        this.respondNodePathOrPrefix = respondNodePathOrPrefix;
    }

    public String getNotifyNodePath() {
        return notifyNodePath;
    }

    public void setNotifyNodePath(String notifyNodePath) {
        this.notifyNodePath = notifyNodePath;
    }

    public String getRespondNodePathOrPrefix() {
        return respondNodePathOrPrefix;
    }

    public void setRespondNodePathOrPrefix(String respondNodePathOrPrefix) {
        this.respondNodePathOrPrefix = respondNodePathOrPrefix;
    }

    @Override
    public String toString() {
        return "SyncRoundPathPair{" +
                "notifyNodePath='" + notifyNodePath + '\'' +
                ", respondNodePathOrPrefix='" + respondNodePathOrPrefix + '\'' +
                '}';
    }
}
