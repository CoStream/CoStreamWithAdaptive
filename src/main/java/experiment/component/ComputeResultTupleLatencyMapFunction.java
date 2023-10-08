package experiment.component;


import base.CommonJoinUnionType;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple4;

/**
 * 输入程序最终连接成功的每个结果元组，根据该元组自带的时间戳以及当前时间，计算出产生该元组的延迟
 */
public class ComputeResultTupleLatencyMapFunction implements MapFunction<CommonJoinUnionType<Tuple4<String, String, String, String>, Tuple4<String, String, String, String>>, Long> {
    @Override
    public Long map(CommonJoinUnionType<Tuple4<String, String, String, String>, Tuple4<String, String, String, String>> value) throws Exception {
        //获取当前时间，采用高性能的方式
        long currentTime = CurrentTimeMillisClock.getInstance().now();
        //输出结果格式 延迟,当前时间戳
        return (currentTime - value.getOtherTimestamp());
    }
}
