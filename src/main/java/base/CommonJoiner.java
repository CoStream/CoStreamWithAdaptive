package base;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;

/**
 * 不同方法的Joiner实现的公共父类，其中包括一些通用的方法
 * @param <F> 第一个输入数据流的泛型
 * @param <S> 第二个输入数据流的泛型
 */
public abstract class CommonJoiner <F,S> extends ProcessFunction<CommonJoinUnionType<F,S>,CommonJoinUnionType<F,S>> implements CheckpointedFunction {
    //R与S中元组保存的时间(即时间窗口大小)
    protected Time R_TimeWindows;
    protected Time S_TimeWindows;

    //两个数据流的键值提取器，键值为Double类型
    protected KeySelector<F,Double> keySelector_R;
    protected KeySelector<S,Double> keySelector_S;

    //用于范围连接，即S - R_behind_S < R < S + R_surpass_S ;R - R_surpass_S < S < R + R_behind_S
    protected double R_surpass_S;
    protected double R_behind_S;

    /**
     * 有参构造器，进行范围连接的相关初始化参数设置
     */
    public CommonJoiner(Time r_TimeWindows, Time s_TimeWindows, KeySelector<F, Double> keySelector_R, KeySelector<S, Double> keySelector_S, double r_surpass_S, double r_behind_S) {
        R_TimeWindows = r_TimeWindows;
        S_TimeWindows = s_TimeWindows;
        this.keySelector_R = keySelector_R;
        this.keySelector_S = keySelector_S;
        R_surpass_S = r_surpass_S;
        R_behind_S = r_behind_S;
    }
}
