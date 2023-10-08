package tools.migration;

import java.io.Serializable;
import java.util.List;

/**
 * 记录单次迁移的方案，即-由谁-向谁-迁移了什么
 */
public class OneMigrationOperation<T extends Serializable> implements Serializable {
    //序列化编号
    private static final long serialVersionUID = 1684553498749L;

    //迁移的源节点编号（迁出的节点）
    private int originalNodeNum;
    //迁移的目标节点编号（迁入的节点）
    private int destinationNodeNum;
    //要迁移的数据的信息
    private T migrationDataInformation;

    public int getOriginalNodeNum() {
        return originalNodeNum;
    }

    public void setOriginalNodeNum(int originalNodeNum) {
        this.originalNodeNum = originalNodeNum;
    }

    public int getDestinationNodeNum() {
        return destinationNodeNum;
    }

    public void setDestinationNodeNum(int destinationNodeNum) {
        this.destinationNodeNum = destinationNodeNum;
    }

    public T getMigrationDataInformation() {
        return migrationDataInformation;
    }

    public void setMigrationDataInformation(T migrationDataInformation) {
        this.migrationDataInformation = migrationDataInformation;
    }
}
