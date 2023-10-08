package coModel.adaptive;

import base.CommonJoinUnionStream;
import base.CommonJoinUnionType;
import coModel.CoModelParameters;
import coModel.adaptive.index.CommonJoinUnionTypeSerializerForAdaptive;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;

public class CoModelUnionStreamWithAdaptive<F, S> extends CommonJoinUnionStream<F, S> {

    private CommonJoinUnionTypeSerializerForAdaptive<F, S> indexSerializer;

    /**
     * 有参构造器，为各个属性赋初始值
     */
    public CoModelUnionStreamWithAdaptive(DataStream<F> firstStream, DataStream<S> secondStream, KeySelector<F, Double> keySelector_R, KeySelector<S, Double> keySelector_S, CommonJoinUnionTypeSerializerForAdaptive<F, S> indexSerializer) {
        super(firstStream, secondStream, keySelector_R, keySelector_S);
        this.indexSerializer = indexSerializer;
    }

    @Override
    public DataStream<CommonJoinUnionType<F, S>> bandNMJoinWithTimeWindow(Time R_TimeWindows, Time S_TimeWindows, double r_surpass_S, double r_behind_S) {
        return joinMethodWithDifferentImplement(
                new CoModelRouterWithAdaptive<>(R_TimeWindows, S_TimeWindows, r_surpass_S, r_behind_S, keySelector_R, keySelector_S),
                new CoModelJoinerWithAdaptive<>(R_TimeWindows, S_TimeWindows, keySelector_R, keySelector_S, r_surpass_S, r_behind_S, indexSerializer),
                CoModelParameters.TOTAL_NUM_OF_ROUTER,
                CoModelParameters.TOTAL_NUM_OF_JOINER);
    }

}
