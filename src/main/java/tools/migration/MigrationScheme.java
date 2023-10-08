package tools.migration;


import org.apache.flink.api.java.tuple.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * 用于指示进行状态迁移时的迁移方案,
 * 其中T表示用于描述要迁移数据的信息类型，一般用 List<Long>类型，表示迁移数据的序列号列表
 */
public class MigrationScheme<T extends Serializable> {
    //保存的迁移方案,表示
    private List<List<T>> migrationScheme;

    //保存全部节点的数量
    private int totalNodeNum;

    /**
     * 初始化
     * @param totalNodeNum 总共的节点数量（不管是不是二部图，此处是所有的Joiner实例数量）
     */
    public MigrationScheme(int totalNodeNum) {
        this.totalNodeNum = totalNodeNum;
        //初始化迁移方案，将其中的项全部置为null
        migrationScheme = new ArrayList<>(totalNodeNum);
        for (int i = 0; i < totalNodeNum; i++) {
            ArrayList<T> item = new ArrayList<>(totalNodeNum);
            for (int j = 0; j < totalNodeNum; j++) {
                item.add(null);
            }
            migrationScheme.add(item);
        }

    }

    /**
     * 向迁移方案中添加一条要执行的迁移记录（原有的对应节点之间的迁移方案会被替换）
     * @param fromNode 迁移的源节点
     * @param toNode   迁移的目标节点
     * @param dataInf  要迁移的数据信息
     */
    public void addOneMigrationOperation(int fromNode, int toNode, T dataInf) {
        migrationScheme.get(fromNode).set(toNode, dataInf);
    }

    /**
     * 获取某个节点的所有的迁出信息，即向哪些节点迁移了哪些数据
     * @param fromNode 要获取迁出信息的节点编号
     * @return List-向哪那个节点迁出，向对应节点的迁出信息-
     */
    public List<Tuple2<Integer, T>> getOutInfOfSpecificMigrateNode(int fromNode) {
        LinkedList<Tuple2<Integer, T>> resultList = new LinkedList<>();
        List<T> fromNodeInf = migrationScheme.get(fromNode);
        for (int i = 0; i < totalNodeNum; i++) {
            T toNodeInf = fromNodeInf.get(i);
            if (toNodeInf != null) {
                resultList.add(new Tuple2<>(i, toNodeInf));
            }
        }
        return resultList;
    }

    /**
     * 获取某个节点的所有的迁入信息，即哪些节点向该节点分别迁移了哪些数据
     * @param toNode 要获取迁入信息的节点编号
     * @return List-哪个节点向该节点迁入，向该节点前入数据的信息-
     */
    public List<Tuple2<Integer, T>> getInInfOfSpecificMigrateNode(int toNode) {
        LinkedList<Tuple2<Integer, T>> resultList = new LinkedList<>();
        for (int i = 0; i < totalNodeNum; i++) {
            List<T> fromNodeInf = migrationScheme.get(i);
            T toNodeInf = fromNodeInf.get(toNode);
            if (toNodeInf != null) {
                resultList.add(new Tuple2<>(i, toNodeInf));
            }
        }
        return resultList;
    }

    /**
     * 获取所有向指定节点迁移数据的源节点的编号的列表
     * @param toNode 迁入节点
     * @return 所有向迁入节点迁移过数据的源节点的列表
     */
    public List<Integer> getOriginalNodeNumOfMigrateInNode(int toNode) {
        LinkedList<Integer> resultList = new LinkedList<>();
        for (int i = 0; i < totalNodeNum; i++) {
            List<T> fromNodeInf = migrationScheme.get(i);
            T toNodeInf = fromNodeInf.get(toNode);
            if (toNodeInf != null) {
                resultList.add(i);
            }
        }
        return resultList;
    }

    /**
     * 打印当前的迁移方案信息，一般仅用于测试
     */
    public void logTestMigrationScheme() {
        for (List item : migrationScheme) {
            for (Object e : item) {
                System.out.print(e);
                System.out.print("|");
            }
            System.out.println();
        }
    }

    public int getTotalNodeNum() {
        return totalNodeNum;
    }

    /**
     * 清空当前迁移方案
     */
    public void clear() {
        for (List<T> e : migrationScheme) {
            e.clear();
        }
        migrationScheme.clear();
    }

    /**
     * 重置当前迁移方案(先清空，后初始化)
     */
    public void reset() {
        clear();
        for (int i = 0; i < totalNodeNum; i++) {
            ArrayList<T> item = new ArrayList<>(totalNodeNum);
            for (int j = 0; j < totalNodeNum; j++) {
                item.add(null);
            }
            migrationScheme.add(item);
        }
    }
}
