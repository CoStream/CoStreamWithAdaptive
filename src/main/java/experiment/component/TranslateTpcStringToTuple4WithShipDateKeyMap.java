package experiment.component;

import common.GeneralParameters;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple4;

public class TranslateTpcStringToTuple4WithShipDateKeyMap implements MapFunction<String, Tuple4<String, String, String, String>> {
    //表明是哪个流
    private String sourceName;
    //保存拆分数组
    private String[] splitStringArr;
    //日期转换器
    private final GetDivDaysOfDate dateTranslator = new GetDivDaysOfDate(GeneralParameters.MIN_SHIP_DATE);

    public TranslateTpcStringToTuple4WithShipDateKeyMap(String sourceName) {
        this.sourceName = sourceName;
    }

    //将字符串转换成之后能够处理的元组形式，元组中所有元素的含义如下：
    //    < 数据源编号【可以为Null，一般无用】-- 键值 -- 时间戳 -- 负载【即原本的整个字符串】>
    @Override
    public Tuple4<String, String, String, String> map(String value) throws Exception {
        splitStringArr = value.split("\\|");
        return new Tuple4<String, String, String, String>(
                sourceName,
                "" + dateTranslator.getDaysOf(splitStringArr[10]),
                "" + System.currentTimeMillis(),
                value);
    }
}
