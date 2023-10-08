package coModel.adaptive;

import common.GeneralParameters;

public class CoModelParameterWithAdaptive {
    // Router中输入速率的监控与上传周期（单位：ms）
    public static final long ROUTER_INPUT_RATE_MONITOR_AND_UPLOAD_PERIOD = 1000;

    // Joiner中占用内存的监控与上传周期（单位：ms）
    public static final long JOINER_SUB_INDEX_MEMORY_MONITOR_AND_UPLOAD_PERIOD = 1000;

    // 每个窗口的子索引数量
    public static final int NUM_OF_SUB_INDEX = GeneralParameters.NUM_OF_SUB_INDEX;

    // 每个窗口的冗余子索引数量,即在正常的子索引数量的基础上，用于防止溢出以及进行复制数调整的子索引数量，至少应为3以防溢出
    public static final int REDUNDANCY_NUM_OF_SUB_INDEX = 8;

    // 协调器收集系统信息（包括Router的输入速率信息以及Joiner的内存占用信息）的周期（单位：ms）
    public static final long COORDINATOR_COLLECT_INFORMATION_PERIOD = 5000;

    // 经计算内存占用过低，导致需要增加复制数增加的次数超过该值时，才考虑增加复制数，以防止波动，
    // 该值乘以上面的周期代表防止波动的具体周期。该值为0则表示每次都是立即增加复制数
    public static final int MAX_CUMULATIVE_NUM_OF_LOW_MEM = 10;

    // 每个子索引的内存占用上限，目前用占据整个子索引总内存的比例表示(比值形式)
    public static final double MAX_SUB_INDEX_MEM_FOOTPRINT_UP_LIMIT_FRACTION = 0.7;
    // 每个子索引的内存占用上限，该值表示绝对值，单位字节，与上面那个值是互相替换的关系(绝对值形式)
    // 该值若小于0，则采用上面的比例进行计算，若大于0，则采用该值
    public static final long MAX_SUB_INDEX_MEM_FOOTPRINT_UP_LIMIT = 20 * 1024 * 1024;

    //是否开启多次调整复制数，即在一个调整复制数周期内是否还可以调整复制数。True：允许在一个调整复制数周期内再次调整复制数
    public static final boolean IS_MULTIPLE_ADJUST_ENABLE = true;

    // 是否开启自适应能力。True：开启自适应能力。
    public static final boolean IS_ADAPTIVE_ENABLE = true;

    // 是否在协调器中开启每次计算后的信息打印功能。 True：开启。
    public static final boolean IS_COORDINATOR_LOG_OPEN = true;

}
