package base;

import java.io.Serializable;

/**
 * 在系统中传递的元组类型，此处为CoModel进行了针对性的设计，加入了序列号成员变量
 */
public class CommonJoinUnionType<F,S> implements Serializable {

    //用于序列化，后加的，序列化也是后加的
    private static final long serialVersionUID = 444L;

    //保存连接的两个流的元组数据
    private F firstType = null;
    private S secondType = null;
    //该联合元组将要被发往的分区
    private int numPartition = 0;

    //该元组对应的时间戳
    private long selfTimestamp;
    //标记该元组是要进行存储还是进行连接(true表示对应的操作类型)
    private boolean storeMode = false;
    private boolean joinMode = false;

    //用于在CoModel中指定该元组的序列号
    private int serialNumOfCoModel = 0;

    //用于后期测试性能的数据
    private long otherTimestamp;


    //无参及有参构造器
    public CommonJoinUnionType() {
    }

    public CommonJoinUnionType(F firstType, S secondType, int numPartition, long selfTimestamp, boolean storeMode, boolean joinMode, int serialNumOfCoModel, long otherTimestamp) {
        this.firstType = firstType;
        this.secondType = secondType;
        this.numPartition = numPartition;
        this.selfTimestamp = selfTimestamp;
        this.storeMode = storeMode;
        this.joinMode = joinMode;
        this.serialNumOfCoModel = serialNumOfCoModel;
        this.otherTimestamp = otherTimestamp;
    }

    //自动生成的所有属性的get，set方法


    public boolean isStoreMode() {
        return storeMode;
    }

    public void setStoreMode(boolean storeMode) {
        this.storeMode = storeMode;
    }

    public boolean isJoinMode() {
        return joinMode;
    }

    public void setJoinMode(boolean joinMode) {
        this.joinMode = joinMode;
    }

    public F getFirstType() {
        return firstType;
    }

    public void setFirstType(F firstType) {
        this.firstType = firstType;
    }

    public S getSecondType() {
        return secondType;
    }

    public void setSecondType(S secondType) {
        this.secondType = secondType;
    }

    public int getNumPartition() {
        return numPartition;
    }

    public void setNumPartition(int numPartition) {
        this.numPartition = numPartition;
    }

    public int getSerialNumOfCoModel() {
        return serialNumOfCoModel;
    }

    public void setSerialNumOfCoModel(int serialNumOfCoModel) {
        this.serialNumOfCoModel = serialNumOfCoModel;
    }

    public long getSelfTimestamp() {
        return selfTimestamp;
    }
    public void setSelfTimestamp(long selfTimestamp) {
        this.selfTimestamp = selfTimestamp;
    }

    public long getOtherTimestamp() {
        return otherTimestamp;
    }

    public void setOtherTimestamp(long otherTimestamp) {
        this.otherTimestamp = otherTimestamp;
    }

    /**
     * 构建一个只包含第一个类型的联合类型，此时第二个类型为null
     * @param ft 第一个类型的元组
     */
    public void one(F ft){
        firstType = ft;
        secondType = (S) null;
    }

    /**
     * 构建一个只包含第二个类型的联合类型，此时第二个类型为null
     * @param st 第二个类型的元组
     */
    public void two(S st){
        firstType = (F) null;
        secondType = st;
    }

    /**
     * 判断当前的元组是否为由第一个类型的元组构成（此时联合类型中第二个类型为null）
     * @return true：该联合类型为由第一个类型的元组构成
     */
    public boolean isOne(){
        return (secondType == null)&&(firstType!=null);
    }



    /**
     * 将当前元组与另一个联合类型的元组合并，合并后的元组包含这两个联合元组的存储的元组信息，同时返回元组的时间参数与调用该方法的元组的参数相同
     * @param other 要联合的另外一个联合类型元组
     * @return 包含这两个联合元组的存储的元组信息的联合类型元组，若要联合的两个元组的类型相同，则返回null
     */
    public CommonJoinUnionType<F,S> union(CommonJoinUnionType<F,S> other){
        //如果两个联合类型元组的类型相同，则返回null
        if(isOne()==other.isOne()){
            return null;
        }
        //如果两个联合类型的类型不容，则构建一个新的包含两个联合类型内容的新联合元组返回，此返回元组的所有时间分区参数与调用该方法的元组的参数相同
        if (isOne()){
            return new CommonJoinUnionType<F, S>(getFirstType(), other.getSecondType(), getNumPartition(), getSelfTimestamp(), isStoreMode(), isJoinMode(), getSerialNumOfCoModel(), getOtherTimestamp());
        }else{
            return new CommonJoinUnionType<F, S>(other.getFirstType(), getSecondType(), getNumPartition(), getSelfTimestamp(), isStoreMode(), isJoinMode(), getSerialNumOfCoModel(), getOtherTimestamp());
        }
    }

    /**
     * 根据指定的元组初始化当前元组，将对应元组的所有属性复制给当前元组
     * @param other 源元组
     */
    public void copyFrom(CommonJoinUnionType<F,S> other) {
        this.firstType = other.getFirstType();
        this.secondType = other.getSecondType();
        this.numPartition = other.getNumPartition();
        this.selfTimestamp = other.getSelfTimestamp();
        this.storeMode = other.isStoreMode();
        this.joinMode = other.isJoinMode();
        this.serialNumOfCoModel = other.getSerialNumOfCoModel();
        this.otherTimestamp = other.getOtherTimestamp();
    }

    @Override
    public String toString() {
        return "CommonJoinUnionType{" +
                "firstType=" + firstType +
                ", secondType=" + secondType +
                ", numPartition=" + numPartition +
                ", selfTimestamp=" + selfTimestamp +
                ", storeMode=" + storeMode +
                ", joinMode=" + joinMode +
                ", serialNumOfCoModel=" + serialNumOfCoModel +
                ", otherTimestamp=" + otherTimestamp +
                '}';
    }

    /**
     * 清空当前元组
     */
    public void clear() {
        this.firstType = null;
        this.secondType = null;
        this.numPartition = 0;
        this.selfTimestamp = 0;
        this.storeMode = false;
        this.joinMode = false;
        this.serialNumOfCoModel = 0;
        this.otherTimestamp = 0;
    }
}
