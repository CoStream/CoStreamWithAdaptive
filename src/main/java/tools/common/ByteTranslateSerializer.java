package tools.common;

import java.io.*;

/**
 * 用于将对象转化成字节数组，以用于数据传输
 * 目前采用的是java自带的序列化器
 * @param <T> 要转换成字节数据的数据类型
 */
public class ByteTranslateSerializer<T extends Serializable> {

    /**
     * 将对象转变成子节数组
     */
    public byte[] serialize(T input) {

        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream oos = null;

        try {
            oos = new ObjectOutputStream(bos);
            oos.writeObject(input);
        } catch (IOException e) {
            e.printStackTrace();
            System.err.println("异常信息 ：ByteTranslateSerializer 中 对象输出流构建失败\n");
        }

        //返回生成的子节数组
        byte[] bytes = bos.toByteArray();

        //关闭流
        try {
            bos.close();
            oos.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return bytes;
    }

    /**
     * 将字节数组反序列化为对象
     */
    public T deserialize(byte[] bytes) {
        ObjectInputStream ois = null;
        T result = null;

        try {
            ois = new ObjectInputStream(new ByteArrayInputStream(bytes));
            result = (T)ois.readObject();
            ois.close();
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
            System.err.println("异常信息 ：ByteTranslateSerializer 中 对象反序列化失败\n");
        }

        return result;
    }

}
