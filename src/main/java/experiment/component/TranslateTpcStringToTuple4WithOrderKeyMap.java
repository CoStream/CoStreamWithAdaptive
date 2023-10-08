package experiment.component;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple4;

/**
 * 将字符串转化为4元组，其中键值为ORDERKEY，即为0位置的值
 */
public class TranslateTpcStringToTuple4WithOrderKeyMap implements MapFunction<String, Tuple4<String, String, String, String>> {
    //表明是哪个流
    private String sourceName;

    public TranslateTpcStringToTuple4WithOrderKeyMap(String sourceName) {
        this.sourceName = sourceName;
    }

    //将字符串转换成之后能够处理的元组形式，元组中所有元素的含义如下：
    //    < 数据源编号【可以为Null，一般无用】-- 键值 -- 时间戳 -- 负载【即原本的整个字符串】>
    @Override
    public Tuple4<String, String, String, String> map(String value) throws Exception {
        //保存拆分数组
        String[] splitStringArr = value.split("\\|");
        return new Tuple4<String, String, String, String>(
                sourceName,
                "" + splitStringArr[0],
                "" + System.currentTimeMillis(),
                value);//整个字符串
    }
}
