package base.subTreeForCoModel;

import base.CommonJoinUnionType;
import org.apache.flink.api.java.tuple.Tuple3;
import org.junit.Test;

import java.util.List;

public class TestForBPlusTreeOfCoModel {
    @Test
    public void test1() {
        SubBPlusTreeForCoModel<CommonJoinUnionType<Integer, Integer>, Double> subTree = new SubBPlusTreeForCoModel<>();
        for (int i = 0; i < 100; i++) {
            CommonJoinUnionType<Integer, Integer> newTuple = new CommonJoinUnionType<>();
            newTuple.one(i);
            newTuple.setSelfTimestamp(i*1000);
            newTuple.setSerialNumOfCoModel(i % 32);

            subTree.insert(newTuple, (double)i, newTuple.getSelfTimestamp());
        }

        List<CommonJoinUnionType<Integer, Integer>> list = subTree.findRangeWithoutTime(55.0, 57.0);

        for (CommonJoinUnionType<Integer, Integer> l : list) {
            System.out.println(l);
        }
        System.out.println("查找数量：" + list.size());

        List<Tuple3<CommonJoinUnionType<Integer, Integer>, Double, Long>> allList = subTree.getList();
        for (Tuple3<CommonJoinUnionType<Integer, Integer>, Double, Long> l : allList) {
            System.out.println(l);
        }
        System.out.println("之前元组总数：" + allList.size());
        System.out.println("之前树的长度：" + subTree.getLength());
        System.out.println("之前树的高度：" + subTree.height);
        System.out.println(subTree.getMinTimestamp());
        System.out.println(subTree.getMaxTimestamp());


        subTree.deleteNodeWithCondition(new DeleteConditionForSubTree<CommonJoinUnionType<Integer, Integer>>() {
            @Override
            public boolean isDeleted(CommonJoinUnionType<Integer, Integer> tuple) {
               // return tuple.getSerialNumOfCoModel() % 2 == 0;
//                return true;
                return tuple.getSerialNumOfCoModel() < 30;
            }
        });

        List<Tuple3<CommonJoinUnionType<Integer, Integer>, Double, Long>> allList2 = subTree.getList();
        for (Tuple3<CommonJoinUnionType<Integer, Integer>, Double, Long> l : allList2) {
            System.out.println(l);
        }
        System.out.println("之后元组总数：" + allList2.size());
        System.out.println("之后树的长度：" + subTree.getLength());
        System.out.println("之后树的高度：" + subTree.height);

        List<CommonJoinUnionType<Integer, Integer>> rangeWithoutTime = subTree.findRangeWithoutTime(95.0, 100.0);
        for (CommonJoinUnionType<Integer, Integer> l : rangeWithoutTime) {
            System.out.println(l);
        }

        System.out.println("删除操作：");
        subTree.deleteNodeWithCondition(new DeleteConditionForSubTree<CommonJoinUnionType<Integer, Integer>>() {
            @Override
            public boolean isDeleted(CommonJoinUnionType<Integer, Integer> tuple) {
                // return tuple.getSerialNumOfCoModel() % 2 == 0;
//                return true;
                return tuple.getSerialNumOfCoModel() < 31;
            }
        });

        List<Tuple3<CommonJoinUnionType<Integer, Integer>, Double, Long>> allList3 = subTree.getList();
        for (Tuple3<CommonJoinUnionType<Integer, Integer>, Double, Long> l : allList3) {
            System.out.println(l);
        }
        System.out.println("之之后元组总数：" + allList3.size());
        System.out.println("之之后树的长度：" + subTree.getLength());
        System.out.println("之之后树的高度：" + subTree.height);


    }
}
