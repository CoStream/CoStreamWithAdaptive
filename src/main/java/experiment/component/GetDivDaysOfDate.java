package experiment.component;

import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;

/**
 * 将指定的日期转换为距离最小日期的天数
 */
public class GetDivDaysOfDate implements Serializable {
    //序列化
    private static final long serialVersionUID = 1L;
    //保存最小日期对应的天数
    private long minDays;
    //用于将毫秒数转换为天数
    private final long trans = 1000 * 60 * 60 * 24;
    //日期格式化器
    private final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");

    public GetDivDaysOfDate(String minDate) {
        long time = 0;
        try {
            time = dateFormat.parse(minDate).getTime();
        } catch (ParseException e) {
            e.printStackTrace();
            System.err.println("GetDivDaysOfDate初始化失败");
        }
        minDays = time / trans - 1;
    }

    /**
     * 返回输入日期对应的距离最小日期的天数，若输入即为最小日期也会返回1，即最小值为1
     * @return 日期对应的距离最小日期的天数
     */
    public long getDaysOf(String date) {
        long days = 1;
        try {
            long time = dateFormat.parse(date).getTime();
            days = time / trans - minDays;
        } catch (ParseException e) {
            e.printStackTrace();
            System.err.println("解析日期错误");
        }
        return days;
    }

}
