package tools.common;

import coModel.CoModelParameters;
import common.GeneralParameters;

/**
 * 用于解析程序的输入参数，通过解析这些参数来更改程序的配置
 * 此时程序的输入参数例子如下：
 *
 * 合成数据集：
 * --rCopyNum 4 --sCopyNum 4 --R_surpass_S 10 --R_behind_S 10
 *
 * TPC数据集：
 * --rCopyNum 4 --sCopyNum 4 --R_surpass_S 1 --R_behind_S 1
 *
 * NexMark数据集：
 * --rCopyNum 8 --sCopyNum 2 --R_surpass_S 0 --R_behind_S 0
 *
 * 目前暂不支持设置并行度
 */
public class ParseExperimentParametersTool {

    //用于保存实验配置的节点，实验启动程序会把实验参数上传到该节点，之后所有组件会从该节点获取配置
    public static final String ZOOKEEPER_EXP_CONFIGS_PATH_OF_ALL_MODEL = "/ExpConfig";

    //用于指示范围连接的参数的参数名
    //r_surpass_S 用于范围连接（以一个S元组的时间为参照物），即S - R_behind_S < R < S + R_surpass_S ;R - R_surpass_S < S < R + R_behind_S
    //r_behind_S 用于范围连接（以一个S元组的时间为参照物），即S - R_behind_S < R < S + R_surpass_S ;R - R_surpass_S < S < R + R_behind_S
    //若要执行等值连接，则该值均为 0
    public static final String PARAM_NAME_OF_R_SURPASS_S = "R_surpass_S";
    public static final String PARAM_NAME_OF_R_BEHIND_S = "R_behind_S";
    //CoModel中R与S的复制数的参数名
    public static final String PARAM_NAME_OF_R_COPY_NUM = "rCopyNum";
    public static final String PARAM_NAME_OF_S_COPY_NUM = "sCopyNum";


    /**
     * 将程序的输入参数转换为字节数组，以便于在Zookeeper中进行数据传输
     * @param args 程序输入参数
     * @return 字符数组转化为字符串后再次转化为字节数组，其中字符串是用“,”分割的
     */
    public static byte[] translateStringArrayToByteArray(String[] args) {
        StringBuilder stringBuilder = new StringBuilder();
        for (String arg : args) {
            stringBuilder.append(arg);
            stringBuilder.append(",");
        }
        return stringBuilder.toString().getBytes();
    }

    /**
     * 将字节数组转化为字符串数组（这个字节数组必须是本类之前转化的字节数组）
     * @param argBytes 字符数组转化为字符串后再次转化为的字节数组，其中字符串是用“,”分割的
     * @return 程序输入参数的字符串数组
     */
    public static String[] translateByteArrayToStringArray(byte[] argBytes) {
        String inputString = new String(argBytes);
        return inputString.split(",");
    }

    /**
     * 根据程序输入参数和要获取的参数名，解析对应的参数值
     * @param args 程序输入参数
     * @param name 要获取的参数名
     * @return 对应的参数值，字符串形式，需要进一步转化
     */
    public static String parseExpParametersForName(String[] args, String name) {
        InputParameterParseTool parseParaTool = new InputParameterParseTool(args);
        return parseParaTool.parseForName(name);
    }

    /**
     * 根据程序输入参数和要获取的参数名，解析对应的参数值，不过该方法输入的是字节数组形式
     * @param argBytes 程序输入参数(字节数组形式)
     * @param name 要获取的参数名
     * @return 对应的参数值，字符串形式，需要进一步转化
     */
    public static String parseExpParametersForNameWithinByteArray(byte[] argBytes, String name) {
        return parseExpParametersForName(translateByteArrayToStringArray(argBytes), name);
    }


}
