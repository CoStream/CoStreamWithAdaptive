package base;

import common.GeneralParameters;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.io.Serializable;

public abstract class CommonJoinUnionStream<F,S> implements Serializable {
    //用于存储两个要连接的数据流
    protected DataStream<F> firstStream;
    protected DataStream<S> secondStream;

    //两个数据流的键值提取器，键值为Double类型
    protected KeySelector<F,Double> keySelector_R;
    protected KeySelector<S,Double> keySelector_S;

    /**
     * 有参构造器，为各个属性赋初始值
     */
    public CommonJoinUnionStream(DataStream<F> firstStream, DataStream<S> secondStream, KeySelector<F, Double> keySelector_R, KeySelector<S, Double> keySelector_S) {
        this.firstStream = firstStream;
        this.secondStream = secondStream;
        this.keySelector_R = keySelector_R;
        this.keySelector_S = keySelector_S;
    }


    /**
     * 将两个输入的数据流包装成元组类型相同的数据流,
     *     在此会设置OtherTimestamp为当前时间，整个join的处理流程为：source->map->map->router->joiner ,
     *     这相当于进入第二个map的时间，可以用来在joiner后记录一个时间后相减，便可得到延迟
     * @param firstInputStream 第一个输入流
     * @param secondInputStream 第二个输入流
     * @return
     */
    protected DataStream<CommonJoinUnionType<F, S>> unionTwoInputStream(DataStream<F> firstInputStream, DataStream<S> secondInputStream) {
        //包装两个输入流为相同的类型
        //将第一个流包装成CommonJoinUnionType<F, S>类型的流
        DataStream<CommonJoinUnionType<F, S>> inputStream1 = firstInputStream.map(new MapFunction<F, CommonJoinUnionType<F, S>>() {
            public CommonJoinUnionType<F, S> map(F value) throws Exception {
                CommonJoinUnionType<F, S> firstUnionType = new CommonJoinUnionType<F, S>();
                firstUnionType.one(value);

                //TODO 测试时间数据设置，（此时不采用高性能的获取时间戳方式）
                firstUnionType.setOtherTimestamp(System.currentTimeMillis());
//                firstUnionType.setOtherTimestamp(CurrentTimeMillisClock.getInstance().now());

                return firstUnionType;
            }
        }).returns(new TypeHint<CommonJoinUnionType<F, S>>() {
            @Override
            public TypeInformation<CommonJoinUnionType<F, S>> getTypeInfo() {
                return super.getTypeInfo();
            }
        }).setParallelism(GeneralParameters.KAFKA_R_TOPIC_PARTITIONS_NUM);    //TODO 并行度与数据源相同


        //将第二个流包装成CommonJoinUnionType<F, S>类型的流
        DataStream<CommonJoinUnionType<F, S>> inputStream2 = secondInputStream.map(new MapFunction<S, CommonJoinUnionType<F, S>>() {
            public CommonJoinUnionType<F, S> map(S value) throws Exception {
                CommonJoinUnionType<F, S> secondUnionType = new CommonJoinUnionType<F, S>();
                secondUnionType.two(value);

                //TODO 测试时间数据设置，（此时不采用高性能的获取时间戳方式）
                secondUnionType.setOtherTimestamp(System.currentTimeMillis());
//                secondUnionType.setOtherTimestamp(CurrentTimeMillisClock.getInstance().now());

                return secondUnionType;
            }
        }).returns(new TypeHint<CommonJoinUnionType<F, S>>() {
            @Override
            public TypeInformation<CommonJoinUnionType<F, S>> getTypeInfo() {
                return super.getTypeInfo();
            }
        }).setParallelism(GeneralParameters.KAFKA_S_TOPIC_PARTITIONS_NUM);    //TODO 并行度与数据源相同


        //合并两个输入流为一个流
        return inputStream1.union(inputStream2);
    }

    /**
     * 范围连接
     * @param R_TimeWindows R窗口中保存元组的时间
     * @param S_TimeWindows S窗口中保存元组的时间
     * @param r_behind_S 用于范围连接（以一个S元组的时间为参照物），即S - R_behind_S < R < S + R_surpass_S ;R - R_surpass_S < S < R + R_behind_S
     * @param r_surpass_S 用于范围连接（以一个S元组的时间为参照物），即S - R_behind_S < R < S + R_surpass_S ;R - R_surpass_S < S < R + R_behind_S
     * @return 连接之后满足结果的输出流,该流中存储的元组类型为联合类型
     */
    public abstract DataStream<CommonJoinUnionType<F,S>> bandNMJoinWithTimeWindow(Time R_TimeWindows, Time S_TimeWindows, double r_surpass_S, double r_behind_S);


    /**
     * 实施不同连接的框架，只有Router与Joiner不同
     * @param router 不同的路由方法
     * @param joiner 不同的连接方法
     * @param numOfRouter 路由节点的数量
     * @param numOfJoiner 连接节点的数量
     * @return 结果数据流
     */
    protected DataStream<CommonJoinUnionType<F, S>> joinMethodWithDifferentImplement(CommonRouter<F, S> router,
                                                                                 CommonJoiner<F, S> joiner,
                                                                                 int numOfRouter,
                                                                                 int numOfJoiner) {
        //合并两个数据流
        DataStream<CommonJoinUnionType<F, S>> unionStream = unionTwoInputStream(firstStream, secondStream);



        //对合并后的数据流进行连接
        return unionStream
//                .partitionCustom(new Partitioner<Integer>() {                         //--自定义分区器(数据源到Router)（后加的内容）
//                    public int partition(Integer key, int numPartitions) {
//                        return key % numPartitions;
//                    }
//                }, new KeySelectorForSourceToRouter<>(keySelector_R,keySelector_S))    //--用于将数据源中的具有相同键值的元组路由到同一个Router中（后加的内容）
                .shuffle()    //将元组从数据源随机分区到所有的Router，是为了保证所有Router之间的平衡（上面的自定义分区器是为了保证所有元组能够按时间顺序到达Joiner）

                .process(router)                                                    //路由
                .setParallelism(numOfRouter)                                        //设置Router并行度
                .partitionCustom(new Partitioner<Integer>() {                       //自定义分区器
                    public int partition(Integer key, int numPartitions) {
                        return key;
                    }
                }, new KeySelector<CommonJoinUnionType<F, S>, Integer>() {
                    public Integer getKey(CommonJoinUnionType<F, S> value) throws Exception {
                        return value.getNumPartition();
                    }
                })
                .process(joiner)                                                    //连接
                .setParallelism(numOfJoiner);                                       //设置Joiner并行度
    }

    /**
     * 用于将具有相同键值的数据源中的元组路由到同一个Router，以此减少Router到Joiner的乱序
     */
    private static class KeySelectorForSourceToRouter<F, S> implements KeySelector<CommonJoinUnionType<F, S>, Integer> {
        //两个数据流的键值提取器，键值为Double类型
        private KeySelector<F,Double> keySelector_R;
        private KeySelector<S,Double> keySelector_S;

        public KeySelectorForSourceToRouter(KeySelector<F, Double> keySelector_R, KeySelector<S, Double> keySelector_S) {
            this.keySelector_R = keySelector_R;
            this.keySelector_S = keySelector_S;
        }
        @Override
        public Integer getKey(CommonJoinUnionType<F, S> value) throws Exception {
            if (value.isOne()) {
                double key = keySelector_R.getKey(value.getFirstType());
                return (int) key;
            } else {
                double key = keySelector_S.getKey(value.getSecondType());
                return (int) key;
            }

        }
    }



}
