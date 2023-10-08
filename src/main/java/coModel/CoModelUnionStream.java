package coModel;

import base.CommonJoinUnionStream;
import base.CommonJoinUnionType;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;

public class CoModelUnionStream<F, S> extends CommonJoinUnionStream<F, S> {
    /**
     * 有参构造器，为各个属性赋初始值
     */
    public CoModelUnionStream(DataStream<F> firstStream, DataStream<S> secondStream, KeySelector<F, Double> keySelector_R, KeySelector<S, Double> keySelector_S) {
        super(firstStream, secondStream, keySelector_R, keySelector_S);
    }

    @Override
    public DataStream<CommonJoinUnionType<F, S>> bandNMJoinWithTimeWindow(Time R_TimeWindows, Time S_TimeWindows, double r_surpass_S, double r_behind_S) {
        return joinMethodWithDifferentImplement(
                new CoModelRouter<>(R_TimeWindows, S_TimeWindows, r_surpass_S, r_behind_S, keySelector_R, keySelector_S),
                new CoModelJoiner<>(R_TimeWindows, S_TimeWindows, keySelector_R, keySelector_S, r_surpass_S, r_behind_S),
                CoModelParameters.TOTAL_NUM_OF_ROUTER,
                CoModelParameters.TOTAL_NUM_OF_JOINER);
    }
}
