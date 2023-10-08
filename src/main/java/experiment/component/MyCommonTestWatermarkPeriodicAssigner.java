package experiment.component;

import common.GeneralParameters;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

public class MyCommonTestWatermarkPeriodicAssigner implements AssignerWithPeriodicWatermarks<Tuple4<String, String, String, String>> {

    private Long bound = GeneralParameters.WATERMARK_AFTER_MAX_TIMESTAMP;//水位线的延迟，ms
    private Long maxTs = 0L;//观察到的最大时间戳
    private Long lastMaxTs = 0L;//上一个最大时间戳，用于控制水位线递增
    @Nullable
    public Watermark getCurrentWatermark() {
        //防止在来数据之前生成水位线导致溢出
        if (maxTs == Long.MIN_VALUE){
            return new Watermark(maxTs);
        }else {
            //确保水位线递增
            if (maxTs < lastMaxTs) {
                maxTs = lastMaxTs;
            } else {
                lastMaxTs = maxTs;
            }
            return new Watermark(maxTs-bound);
        }
//            return new Watermark(maxTs-bound);
    }

    public long extractTimestamp(Tuple4<String, String, String, String> element, long recordTimestamp) {
        long timestamp = Long.parseLong(element.f2);
        if (timestamp>maxTs){
            maxTs = timestamp;
        }
        return timestamp;
    }
}
