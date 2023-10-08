package experiment.component;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

/**
 * 分别累计每个Joiner输出结果每一秒内的平均延迟，以并行的方式运行，并行度与Joiner数相同
 *      该方法和非精确的方法不同，在该方法中直接采用系统时间确定当前时间戳，而不是通过自定义的获取时间类
 *      该方法的并行能力可能会下降，但是时间是准确的
 *     输出结果格式 ： 周期内元组数，平均延迟，周期开始时间，周期结束时间
 */
public class ParallelAccumulateResultLatencyMapFunctionPrecise extends RichFlatMapFunction<Long,String> {
    // 上一次输出结果的时间
    private Long lastComputeTime;
    // 输出结果的周期(ms)
    private final Long period = 1000L;

    // 周期内接收到的元组的累计总时间
    private double sumTime;
    // 周期内接收到的元组的个数
    private long tupleNums;

    @Override
    public void open(Configuration parameters) throws Exception {
        //获取当前时间
        lastComputeTime = System.currentTimeMillis();
        //初始化
        sumTime = 0.0;
        tupleNums = 0;
    }


    @Override
    public void flatMap(Long value, Collector<String> out) throws Exception {
        // 累计延迟
        sumTime += value;
        tupleNums++;

        // 如果距离上一次输出结果的时间间隔达到预定义的周期
        long currentTime = System.currentTimeMillis();
        if (currentTime >= (lastComputeTime + period)) {
            // 输出结果格式 ： 周期内元组数，平均延迟，周期开始时间，周期结束时间
            out.collect("" + tupleNums + "," + (sumTime / tupleNums) + "," + lastComputeTime + "," + currentTime);
            // 重置
            lastComputeTime = currentTime;
            sumTime = 0;
            tupleNums = 0;
        }
    }
}
