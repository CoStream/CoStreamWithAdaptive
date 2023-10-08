package tools.common;

/**
 * 用于解析jar包的输入参数
 */
public class InputParameterParseTool {
    //保存程序的输入参数
    private String[] args;

    public InputParameterParseTool(String[] args) {
        this.args = args;
    }

    /**
     * 根据给出的要获取的参数名，返回对应的参数
     * @param name 参数名称
     * @return 参数对应的具体值
     */
    public String parseForName(String name) {
        boolean flag = false;
        for (String input : args) {
            if (flag) {
                return input;
            }
            if (input.equals("--" + name)) {
                flag = true;
            }
        }
        return null;
    }


    /**
     * 获取完整的参数字符串
     * @return 完整的参数字符串
     */
    public String getAllArgsString() {
        StringBuilder stringBuilder = new StringBuilder();
        for (String input : args) {
            stringBuilder.append(input).append(" ");
        }
        return stringBuilder.toString();
    }


}
