package experiment.component;


import base.CommonJoinUnionType;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple4;

/**
 * 输入程序最终连接成功的每个结果元组，根据该元组自带的时间戳以及当前时间，计算出产生该元组的延迟
 * 该方法获取当前时间的方式是读取系统时间，因此与另一个方法比较，该方法计算的时间是精确的
 */
public class ComputeResultTupleLatencyMapFunctionPrecise implements MapFunction<CommonJoinUnionType<Tuple4<String, String, String, String>, Tuple4<String, String, String, String>>, Long> {
    @Override
    public Long map(CommonJoinUnionType<Tuple4<String, String, String, String>, Tuple4<String, String, String, String>> value) throws Exception {

        //输出结果格式： 当前元组的处理延迟
        return (System.currentTimeMillis() - value.getOtherTimestamp());

    }
}
