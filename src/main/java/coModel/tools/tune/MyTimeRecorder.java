package coModel.tools.tune;

import java.io.Serializable;

/**
 * 计算某段操作的耗时（单位：纳秒nm）
 * 使用方法是在要计时的操作前调用start方法，在结束后调用stopAndReturn方法
 * 该类可以设置每隔几次操作才进行一次计时
 */
public class MyTimeRecorder implements Serializable {
    //要记录的操作的开始时间和结束时间
    private long startTime;
    private long stopTime;
    //记录现在是上一次测量时间后的第几次测量，用于设置跳过某些测量
    private long count = 0;
    //设置每多少次操作才记录一次时间，该值在对象构建时就被确定
    //例如：若值为1，则表示每次操作都会被记录；若为2，则隔一次记录一个
    private long recordGap = 1;

    /**
     * 无参构造，默认每次都会记录时间
     */
    public MyTimeRecorder() {
        this(1);
    }

    /**
     * 有参构造，设置每隔几次操作才进行一次计时
     * 例如：若值为1，则表示每次操作都会被记录；若为2，则隔一次记录一个
     * @param recordGap 每隔几次操作才进行一次计时
     */
    public MyTimeRecorder(long recordGap) {
        this.recordGap = recordGap;
    }

    /**
     * 开始计时，该方法在要记录的操作前调用
     * 注释掉的方法是实现的每隔几个才记录一次，以减小记录频率，但实际并没什么用处，所以去掉
     */
//    public void startRecord() {
//        count++;  //更新计数
//        if (count >= recordGap) {
//            startTime = System.nanoTime();
//        }
//    }
    public void startRecord() {
        startTime = System.nanoTime();
    }

    /**
     * 结束计时，并返回这段操作的时间，该方法在要记录的操作后调用
     * @return 返回记录的操作持续时间（单位：纳秒），如果本次记录属于被跳过的记录，则返回-1（返回-1的部分已经被注释掉）
     * 注释掉的方法是实现的每隔几个才记录一次，以减小记录频率，但实际并没什么用处，所以去掉
     */
//    public long stopRecordAndReturn() {
//        if (count >= recordGap) {
//            stopTime = System.nanoTime();
//            count = 0;  //清零计数
//            return stopTime - startTime;
//        } else {
//            return -1;
//        }
//    }
    public long stopRecordAndReturn() {
        stopTime = System.nanoTime();
        return stopTime - startTime;
    }
}
