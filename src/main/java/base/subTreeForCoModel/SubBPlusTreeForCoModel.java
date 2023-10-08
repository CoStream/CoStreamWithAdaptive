package base.subTreeForCoModel;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;


/**
 *  用于CoModel的B+树类型，主要加入了根据某些条件删除特定叶子节点的方法
 * @param <T> 要存储的元组类型
 * @param <K> 元组对应的键值
 */
public class SubBPlusTreeForCoModel<T,K extends Comparable<K>> {
    //B+树的阶
    private Integer bTreeOrder;


    //当前B+树中存储的元组的数量
    private int length = 0;
    //底层有序链表的头节点
    private LinkListNode first;
    //底层有序链表的尾节点
    private LinkListNode last;

    //当前B+树存储的元组的最小以及最大的时间戳
    private Long minTimestamp;
    private Long maxTimestamp;

    //根节点
    private Node root;

    //树高，没什么用，可删除
    int height =0;

    /**
     * 无参构造方法，默认阶为3，内部调用有参构造方法
     */
    public SubBPlusTreeForCoModel() {
        this(3);
    }

    /**
     * 有参构造方法，可以设置B+树的阶，并进行相关的初始化
     * @param bTreeOrder B+树的阶
     */
    public SubBPlusTreeForCoModel(Integer bTreeOrder) {
        this.bTreeOrder = bTreeOrder;
        this.root = new LeafNode();

        first = new LinkListNode(null, null, null, null, null, null);
        last = new LinkListNode(null, null, null, null, null, null);

        first.next = last;
        last.prev = first;

        minTimestamp = Long.MAX_VALUE;
        maxTimestamp = Long.MIN_VALUE;
    }

    /**
     * 利用一个 List<Tuple3<T,K,Long>> 的列表构建B+树，在读写检查点时会被调用
     * @param list 用于构建B+树的列表
     * @param bTreeOrder B+树的阶
     */
    public SubBPlusTreeForCoModel(List<Tuple3<T,K,Long>> list , Integer bTreeOrder){
        //调用构造器指定B+树的阶
        this(bTreeOrder);

        //将List中的所有元组插入到当前B+树中
        for (Tuple3<T,K,Long> t : list){
            insert(t.f0,t.f1,t.f2);
        }

    }

    /**
     * 将底层链表作为一个 List 返回
     * @return 存储了所有底层链表元素的List(元组，键值，时间戳)
     */
    public List<Tuple3<T,K,Long>> getList(){
        List<Tuple3<T,K,Long>> resultList = new ArrayList<Tuple3<T, K, Long>>();
        LinkListNode next = first.next;
        while (next.next!=null){
            resultList.add(new Tuple3<T, K, Long>(next.item, next.key, next.timestamp));
            next=next.next;
        }
        return resultList;
    }

    /**
     * 获取当前B+树底层存储所有数据的链表的头部节点
     * @return 当前B+树底层存储所有数据的链表的头部节点
     */
    public LinkListNode getFirst() {
        return first;
    }

    /**
     * 获取存储数据的个数
     * @return 当前B+树中存储的元组的个数
     */
    public int getLength() {
        return length;
    }

    /**
     * 获取当前B+树所有存储的元组中的最小时间戳
     * @return 当前B+树所有存储的元组中的最小时间戳
     */
    public Long getMinTimestamp() {
        return minTimestamp;
    }

    /**
     * 获取当前B+树所有存储的元组中的最大时间戳
     * @return 当前B+树所有存储的元组中的最大时间戳
     */
    public Long getMaxTimestamp() {
        return maxTimestamp;
    }

    /**
     * 范围查找
     * @param minKey 范围下界
     * @param maxKey 范围上界
     * @param timestamp 所要查找的最小时间戳
     * @return 所有满足时间条件及范围的结果所构成的链表
     */
    public List<T> findRange(K minKey,K maxKey,Long timestamp){

        //如果要查询的最小键值大于最大键值，则返回null
        if (minKey.compareTo(maxKey)>0){
            return null;
        }

        if (length == 0) {
            return null;
        }

        return this.root.findRange(minKey,maxKey,timestamp);
    }

    /**
     * 不考虑时间的范围查找（用于当前B+树的所有元组均满足时间要求的情况）
     * @param minKey 范围下界
     * @param maxKey 范围上界
     * @return 所有满足范围的结果所构成的链表
     */
    public List<T> findRangeWithoutTime(K minKey,K maxKey){

        //如果要查询的最小键值大于最大键值，则返回null
        if (minKey.compareTo(maxKey)>0){
            return null;
        }

        if (length == 0) {
            return null;
        }

        return this.root.findRangeWithoutTime(minKey,maxKey);
    }

    /**
     * 插入
     * @param tuple 插入的元组原始数据
     * @param key 插入的元组键值
     * @param timestamp 插入的元组时间戳
     */
    public void insert(T tuple,K key , Long timestamp){

        if(key == null){
            return;
        }

        //更新最小及最大时间戳
        if (timestamp < minTimestamp){
            minTimestamp = timestamp;
        }
        if (timestamp > maxTimestamp){
            maxTimestamp = timestamp;
        }

        Node insertNode = this.root.insert(tuple, key, timestamp,0,null);
        if(insertNode != null){
            this.root = insertNode;
            height++;
        }
    }

    /**
     * 移除此树种范围在minKey与maxKey（包含左边界minKey不包含右边界maxKey）之间的所有存储的元组，并且将这些元组以及其对应的时间戳返回
     *      注意，是[minKey,maxKey),不包含右边界maxKey
     * @return 范围在minKey与maxKey之间的所有存储的元组 以及 其对应的时间戳
     */
    public List<Tuple2<T, Long>> removeKeyRange(K minKey, K maxKey) {
        //如果要查询的最小键值大于最大键值，则返回null
        if (minKey.compareTo(maxKey)>0){
            return null;
        }

        if (length == 0) {
            return null;
        }

        //找到范围内最小的键值对应的最左侧底层节点
        LinkListNode currentLinkedNode = root.findMinLinkedNodeWithKey(minKey, maxKey);

        if (currentLinkedNode == null) {
            return null;
        }

        //结果列表
        LinkedList<Tuple2<T, Long>> resultList = new LinkedList<>();

        //当前节点不能为尾节点，如果当前节点的键值小于等于最大键值，则将其删除
        //    此处不包含右边界max，因此后面的比较项不能等于0
        while (currentLinkedNode.next != null && currentLinkedNode.key.compareTo(maxKey) < 0) {
            //添加新节点，即元组，时间戳
            resultList.add(new Tuple2<>(currentLinkedNode.item, currentLinkedNode.timestamp));
            //保存前向，后向指针
            LinkListNode prevNode = currentLinkedNode.prev;
            LinkListNode nextNode = currentLinkedNode.next;
            //在叶子节点中删除当前节点
            currentLinkedNode.toParent.removeLeafNode(currentLinkedNode);
            //从底层链表中删除该节点
            prevNode.next = nextNode;
            nextNode.prev = prevNode;
            //销毁该节点
            currentLinkedNode.discard();
            //该节点指向下一个节点
            currentLinkedNode=nextNode;
        }
        return resultList;
    }

    /**
     * 根据指定的条件删除叶子节点
     * 便利所有叶子节点，删除其中所有满足条件的节点
     * @param condition 要删除的节点满足的条件
     */
    public void deleteNodeWithCondition(DeleteConditionForSubTree<T> condition) {
        //从底层节点的头部开始便利
        LinkListNode currentLinkedNode = first.next;

        //一直遍历到底层链表的结尾
        while (currentLinkedNode.next != null) {

            //保存前向，后向指针
            LinkListNode prevNode = currentLinkedNode.prev;
            LinkListNode nextNode = currentLinkedNode.next;

            if (condition.isDeleted(currentLinkedNode.item)) {  //如果满足条件，则删除该节点，在方法内部已经减去了树的长度

                //在叶子节点中删除当前节点
                currentLinkedNode.toParent.removeLeafNode(currentLinkedNode);

                //从底层链表中删除该节点
                prevNode.next = nextNode;
                nextNode.prev = prevNode;

                //销毁该节点
                currentLinkedNode.discard();
            }  //end if

            //该节点指向下一个节点
            currentLinkedNode = nextNode;

        } // end while
    }

    /**
     * 清除该子树，删除其中的所有指针
     */
    public void clear(){

        //从根节点开始删除所有的索引结构
        this.root.clear();

        //删除底层链表
        LinkListNode e = first;
        while(e != null){
            LinkListNode next = e.next;
            e.discard();
            e = next;
        }
        first = null;
        last = null;

        //删除相关的属性
        this.maxTimestamp = null;
        this.minTimestamp = null;
        this.length = 0;
        this.height = 0;
        this.bTreeOrder = null;

    }

    /**
     * 内部节点以及叶子节点的公共父类
     *
     */
    abstract class Node{
        protected Node toParent; //指向父节点的指针
        protected K rightKeyForParent;    //在父节点中保存的，代表其最右端的键值
        protected int size;   //叶子节点以及内部非叶子节点所含键值的数量
        protected ArrayList<K> keys;  //保存叶子节点以及内部非叶子节点所含键值

        /**
         * 范围查找
         * @param minKey 范围下界
         * @param maxKey 范围上界
         * @param timestamp 所要查找的最小时间戳
         * @return 所有满足时间条件及范围的结果所构成的链表
         */
        abstract List<T> findRange(K minKey,K maxKey,Long timestamp);

        /**
         * 不考虑时间的范围查找（用于当前B+树的所有元组均满足时间要求的情况）
         * @param minKey 范围下界
         * @param maxKey 范围上界
         * @return 所有满足范围的结果所构成的链表
         */
        abstract List<T> findRangeWithoutTime(K minKey,K maxKey);

        /**
         * 插入一个元组
         * @param tuple 插入的实际元组
         * @param key 插入元组的键值
         * @param timestamp 插入元组的时间戳
         * @param position 调用当前方法的父节点的插入位置
         * @param parent 调用当前方法的父节点
         * @return
         */
        abstract Node insert(T tuple,K key,Long timestamp ,int position,InnerNode parent);

        /**
         * 清空该节点以及所有的子节点
         */
        abstract void clear();

        /**
         * 删除子树中对应的子节点，如果删除完后该节点没有子节点，则向上递归删除
         * @param deleteNode 要删除的子节点的指针
         */
        abstract void removeChildNode(Node deleteNode);

        /**
         * 根据指定的key范围找到指定的范围内最小键值底层节点，一般用于removeChildNode方法中找到第一个要删除的节点
         *
         * @return 指定键值范围的最左侧的节点
         */
        abstract LinkListNode findMinLinkedNodeWithKey(K minKey, K maxKey);

    }

    /**
     * 内部节点（非叶子）
     *
     */
    class InnerNode extends Node{

        //保存指向当前内部节点所拥有的所有子节点的指针
        private ArrayList<Node> childs;

        public InnerNode() {
            this.size = 0;
            this.childs = new ArrayList<Node>();
            this.keys = new ArrayList<K>();
        }

        /**
         * 范围查找
         * @param minKey 范围下界
         * @param maxKey 范围上界
         * @param timestamp 所要查找的最小时间戳
         * @return 所有满足时间条件及范围的结果所构成的链表
         */
        List<T> findRange(K minKey, K maxKey, Long timestamp) {

            //当所要查找的最小值都比当前节点已经存储的最大值大时，查询结果为空
            if(minKey.compareTo(this.keys.get(this.keys.size()-1))>0){
                return null;
            }

            //destIndex为当前要查找的minKey在子节点当中的存储位置
            int destIndex;

            //二分法查找要进行查找的子节点
            int minIndex = 0;
            int maxIndex = this.childs.size() - 1;
            int middle;

            while(minIndex < maxIndex-1){
                middle = ( minIndex + maxIndex )/2;
                if (minKey.compareTo(this.keys.get(middle))>0){//当前要插入的键值大于存储键值的中间位置的键值
                    minIndex = middle;
                }else{
                    maxIndex = middle;
                }
            }

            if (minKey.compareTo(this.keys.get(minIndex))<=0){
                destIndex = minIndex;
            }else {
                destIndex = maxIndex;
            }

            //找到下一个要搜索的子节点
            Node nextSearchNode = this.childs.get(destIndex);

            //返回子节点的查询结果
            return nextSearchNode.findRange(minKey,maxKey,timestamp);
        }

        /**
         * 不考虑时间的范围查找（用于当前B+树的所有元组均满足时间要求的情况）
         * @param minKey 范围下界
         * @param maxKey 范围上界
         * @return 所有满足范围的结果所构成的链表
         */
        List<T> findRangeWithoutTime(K minKey, K maxKey) {
            //当所要查找的最小值都比当前节点已经存储的最大值大时，查询结果为空
            if(minKey.compareTo(this.keys.get(this.keys.size()-1))>0){
                return null;
            }

            //destIndex为当前要查找的minKey在子节点当中的存储位置
            int destIndex;

            //二分法查找要进行查找的子节点
            int minIndex = 0;
            int maxIndex = this.childs.size() - 1;
            int middle;

            while(minIndex < maxIndex-1){
                middle = ( minIndex + maxIndex )/2;
                if (minKey.compareTo(this.keys.get(middle))>0){//当前要插入的键值大于存储键值的中间位置的键值
                    minIndex = middle;
                }else{
                    maxIndex = middle;
                }
            }

            if (minKey.compareTo(this.keys.get(minIndex))<=0){
                destIndex = minIndex;
            }else {
                destIndex = maxIndex;
            }

            //找到下一个要搜索的子节点
            Node nextSearchNode = this.childs.get(destIndex);

            //返回子节点的查询结果
            return nextSearchNode.findRangeWithoutTime(minKey,maxKey);
        }

        /**
         * 内部非叶子节点的数据插入，此方法会递归调用
         * @param tuple 插入的实际元组
         * @param key 插入元组的键值
         * @param timestamp 插入元组的时间戳
         * @param position 调用当前方法的父节点的插入位置
         * @param parent 调用当前方法的父节点
         * @return 如果产生了新的根节点，则返回该根节点，否则返回null
         */
        Node insert(T tuple, K key, Long timestamp ,int position,InnerNode parent) {

            Node nextInsertNode;//接下来要进行插入操作的子节点
            //destIndex为当前要插入的位置
            int destIndex;

            if (key.compareTo(this.keys.get(this.keys.size()-1))>0){//当当前要插入的键值比当前节点已存储的最大的键值都大时

                //更新当前节点所存储键值的最大值
                this.keys.set(this.keys.size()-1,key);

                this.rightKeyForParent = key;

                //下一个要进行插入的节点是最后的一个子节点
                destIndex = this.keys.size()-1;
                nextInsertNode = this.childs.get(destIndex);

            }else{//要插入的键值不大于已存储的最大键值
                //使用二分搜索法搜索插入的位置
                int minIndex = 0;
                int maxIndex = this.childs.size() - 1;
                int middle;

                while(minIndex < maxIndex-1){
                    middle = ( minIndex + maxIndex )/2;
                    if (key.compareTo(this.keys.get(middle))>0){//当前要插入的键值大于存储键值的中间位置的键值
                        minIndex = middle;
                    }else{
                        maxIndex = middle;
                    }
                }


                if (key.compareTo(this.keys.get(minIndex))<=0){
                    destIndex = minIndex;
                }else {
                    destIndex = maxIndex;
                }

                //获取要进行插入的子节点
                nextInsertNode = this.childs.get(destIndex);

            }

            //调用子节点的插入方法
            nextInsertNode.insert(tuple,key,timestamp,destIndex,this);

            //判断当前内部节点需不需要分裂
            if (this.keys.size()>bTreeOrder){ //当前内部节点保存的键值数量大于B+树的阶，需要分裂

                InnerNode newInnerNode = new InnerNode(); //新建一个内部节点，作为当前节点左侧的节点
                int leftSize = this.keys.size()/2; //分裂之后左侧节点的大小
                int rightSize = this.keys.size() - leftSize;
                for (int i = 0;i<leftSize;i++){
                    //将当前内部节点左半部分的存储数据保存到新建的节点中
                    //并更改父节点指针
                    newInnerNode.keys.add(this.keys.remove(0));
                    Node removeNode = this.childs.remove(0);
                    removeNode.toParent=newInnerNode;
                    newInnerNode.childs.add(removeNode);

                }

                //更新大小
                newInnerNode.size = leftSize;
                this.size = rightSize;

                if (parent==null){//当前叶子节点没有父节点，则新建一个返回

                    //新建一个父节点
                    InnerNode parentInnerNode = new InnerNode();
                    //将新建的节点以及当前节点插入到新建的内部节点当中
                    K newRightKey = newInnerNode.keys.get(newInnerNode.keys.size() - 1);
                    newInnerNode.rightKeyForParent = newRightKey;
                    parentInnerNode.keys.add(newRightKey);
                    parentInnerNode.childs.add(newInnerNode);

                    K thisRightKey = this.keys.get(this.keys.size() - 1);
                    this.rightKeyForParent = thisRightKey;
                    parentInnerNode.keys.add(thisRightKey);
                    parentInnerNode.childs.add(this);

                    //更新两个节点的父指针
                    newInnerNode.toParent = parentInnerNode;
                    this.toParent = parentInnerNode;


                    parentInnerNode.size = 2;
                    //返回新建的父节点
                    return parentInnerNode;

                }else {//当前叶子节点有父节点，则将当前节点插入到父节点中

                    //插入到调用此方法的内部节点的位置
                    K newRightKey = newInnerNode.keys.get(newInnerNode.keys.size() - 1);
                    newInnerNode.rightKeyForParent = newRightKey;
                    parent.keys.add(position, newRightKey);
                    parent.childs.add(position,newInnerNode);

                    newInnerNode.toParent = parent;

                    //父节点的size加一
                    parent.size++;

                    return null;
                }

            }else {//当前节点不需要分裂
                return null;
            }


        }

        /**
         * 清空该内部非叶子节点
         */
        void clear() {
            //清空所有的子节点
            for(Node child : childs){
                child.clear();
            }

            //清空该节点
            this.childs.clear();
            this.keys.clear();
            this.toParent = null;
            this.size=0;
        }

        @Override
        void removeChildNode(Node deleteNode) {
            for (int i = 0; i < this.childs.size(); i++) {
                if (this.childs.get(i) == deleteNode) {
                    this.keys.remove(i);
                    this.childs.remove(i);

                    this.size--;

                    //如果当前节点为空，则从父节点中删除当前节点
                    if (this.childs.isEmpty()) {
                        //如果当前节点即是根节点，则代表树空了，则重新建树，根节点初始化
                        if (this == root) {
                            root = new LeafNode();
                            return;
                        }

                        //从父节点中删除
                        this.toParent.removeChildNode(this);

                        //删除当前节点
                        this.toParent=null;
                        this.keys=null;
                        this.childs=null;
                        this.rightKeyForParent=null;

                    }

                    return;
                }//end if
            }//end for
        }

        @Override
        LinkListNode findMinLinkedNodeWithKey(K minKey, K maxKey) {
            //当所要查找的最小值都比当前节点已经存储的最大值大时，查询结果为空
            if(minKey.compareTo(this.keys.get(this.keys.size()-1))>0){
                return null;
            }

            //destIndex为当前要查找的minKey在子节点当中的存储位置
            int destIndex;

            //二分法查找要进行查找的子节点
            int minIndex = 0;
            int maxIndex = this.childs.size() - 1;
            int middle;

            while(minIndex < maxIndex-1){
                middle = ( minIndex + maxIndex )/2;
                if (minKey.compareTo(this.keys.get(middle))>0){//当前要插入的键值大于存储键值的中间位置的键值
                    minIndex = middle;
                }else{
                    maxIndex = middle;
                }
            }

            if (minKey.compareTo(this.keys.get(minIndex))<=0){
                destIndex = minIndex;
            }else {
                destIndex = maxIndex;
            }

            //找到下一个要搜索的子节点
            Node nextSearchNode = this.childs.get(destIndex);

            //返回子节点的查询结果
            return nextSearchNode.findMinLinkedNodeWithKey(minKey,maxKey);
        }


    }

    /**
     * 叶子节点
     *
     */
    class LeafNode extends Node{

        //存储其指向的底层有序链表
        ArrayList<LinkListNode> childs;

        public LeafNode() {
            this.size=0;
            this.childs = new ArrayList<LinkListNode>();
            this.keys = new ArrayList<K>();

        }

        /**
         * 范围查找
         * @param minKey 范围下界
         * @param maxKey 范围上界
         * @param timestamp 所要查找的最小时间戳
         * @return 所有满足时间条件及范围的结果所构成的链表
         */
        List<T> findRange(K minKey, K maxKey, Long timestamp) {

            //返回的满足连接条件及时间要求的结果列表
            List<T> resultList = new ArrayList<T>();

            //获取当前叶子节点指向的第一个（即key值最小）的底层链表节点的指针
            LinkListNode currentLinkListNode = this.childs.get(0);

            //跳过前面所有key小于minKey的节点,此后的节点key均大于等于minKey。(当前节点不能为尾节点)
            while (currentLinkListNode.next!=null && currentLinkListNode.key.compareTo(minKey)<0){
                currentLinkListNode=currentLinkListNode.next;
            }

            //找寻所有key大于等于minKey的节点中，key小于等于maxKey的节点。(当前节点不能为尾节点)
            while (currentLinkListNode.next!=null && currentLinkListNode.key.compareTo(maxKey)<=0){
                //如果满足时间要求（即在指定的时间戳之后到达），则将当前节点当中存储的数据放入返回链表中
                if (currentLinkListNode.timestamp>=timestamp){
                    resultList.add(currentLinkListNode.item);
                }
                //这个地方原本是个BUG，找了好久终于找到了，不管时间戳满不满足要求，都要把指针后移一位
                currentLinkListNode = currentLinkListNode.next;

            }

            return resultList;
        }

        /**
         * 不考虑时间的范围查找（用于当前B+树的所有元组均满足时间要求的情况）
         * @param minKey 范围下界
         * @param maxKey 范围上界
         * @return 所有满足范围的结果所构成的链表
         */
        List<T> findRangeWithoutTime(K minKey, K maxKey) {
            //返回的满足连接条件及时间要求的结果列表
            List<T> resultList = new ArrayList<T>();

            //获取当前叶子节点指向的第一个（即key值最小）的底层链表节点的指针
            LinkListNode currentLinkListNode = this.childs.get(0);

            //跳过前面所有key小于minKey的节点,此后的节点key均大于等于minKey。(当前节点不能为尾节点)
            while (currentLinkListNode.next!=null && currentLinkListNode.key.compareTo(minKey)<0){
                currentLinkListNode=currentLinkListNode.next;
            }

            //找寻所有key大于等于minKey的节点中，key小于等于maxKey的节点。(当前节点不能为尾节点)
            while (currentLinkListNode.next!=null && currentLinkListNode.key.compareTo(maxKey)<=0){

                //不考虑时间限制，直接将满足条件的元组放入到返回列表中
                resultList.add(currentLinkListNode.item);
                currentLinkListNode = currentLinkListNode.next;


            }

            return resultList;
        }

        /**
         * 向叶子节点插入数据，叶子节点用 ArrayList 保存指向最低层实际存储数据节点 LinkListNode 的指针
         * 最低层实际存储数据节点 LinkListNode 中的数据根据 key 值升序排列
         * @param tuple 插入的实际元组
         * @param key 插入元组的键值
         * @param timestamp 插入元组的时间戳
         * @param position 调用当前方法的父节点的插入位置
         * @param parent 调用当前方法的父节点
         * @return 若当前叶子节点没有父节点，则返回 null ，否则，新建一个内部节点 InnerNode 作为当前叶子节点的父节点返回
         */
        Node insert(T tuple, K key, Long timestamp ,int position,InnerNode parent) {

            //构建底层存储列表的数据节点
            LinkListNode newLinkListNode = new LinkListNode(tuple, key, timestamp, null, null, this);

            //当向整个B+树中第一次添加数据时
            if (length == 0){

                //添加节点以及键值
                keys.add(key);
                childs.add(newLinkListNode);

                //链接底部链表
                first.next=newLinkListNode;
                newLinkListNode.prev=first;

                last.prev = newLinkListNode;
                newLinkListNode.next=last;

                //更新当前叶子节点的大小以及整个B+树的长度
                this.size++;
                length++;

                this.rightKeyForParent = key;

                return null;
            }

            //当不是向整个B+树中第一次添加数据时

            if (key.compareTo(this.keys.get(this.keys.size()-1))>0){//当当前要插入的键值比当前叶子节点已经存储的最大键值还大时
                //保存原来的最大键值的元组
                LinkListNode lastLinkListNode = this.childs.get(this.keys.size()-1);
                //插入新的节点
                this.keys.add(key);
                this.childs.add(newLinkListNode);
                //链接底层链表
                lastLinkListNode.next.prev = newLinkListNode;
                newLinkListNode.next = lastLinkListNode.next;
                lastLinkListNode.next = newLinkListNode;
                newLinkListNode.prev = lastLinkListNode;

                //更新当前叶子节点的大小以及整个B+树的长度
                this.size++;
                length++;

                this.rightKeyForParent = key;

            }else{
                //使用二分搜索法搜索插入的位置
                int minIndex = 0;
                int maxIndex = this.childs.size() - 1;
                int middle;

                while(minIndex < maxIndex-1){
                    middle = ( minIndex + maxIndex )/2;
                    if (key.compareTo(this.keys.get(middle))>0){//当前要插入的键值大于存储键值的中间位置的键值
                        minIndex = middle;
                    }else{
                        maxIndex = middle;
                    }
                }

                //destIndex为当前要插入的位置,目前其中保存着要插入节点的后继节点
                int destIndex;
                if (key.compareTo(this.keys.get(minIndex))<=0){
                    destIndex = minIndex;
                }else {
                    destIndex = maxIndex;
                }

                //保存当前要插入节点的后继节点
                LinkListNode lastLinkListNode = this.childs.get(destIndex);

                //插入新的节点
                this.keys.add(destIndex,key);
                this.childs.add(destIndex,newLinkListNode);

                //链接底层链表
                lastLinkListNode.prev.next = newLinkListNode;
                newLinkListNode.prev = lastLinkListNode.prev;
                lastLinkListNode.prev = newLinkListNode;
                newLinkListNode.next = lastLinkListNode;

                //更新当前叶子节点的大小以及整个B+树的长度
                this.size++;
                length++;

            }


            //判断当前叶子节点需不需要分裂
            if (this.keys.size()>bTreeOrder){ //当前叶子节点保存的键值数量大于B+树的阶，需要分裂

                LeafNode newLeafNode = new LeafNode(); //新建一个叶子节点，作为当前节点左侧的节点
                int leftSize = this.keys.size()/2; //分裂之后左侧节点的大小
                int rightSize = this.keys.size() - leftSize;
                for (int i = 0;i<leftSize;i++){
                    //将当前叶子节点左半部分的存储数据保存到新建的节点中
                    //改变这些改变了父节点的底层节点的指针
                    LinkListNode node = this.childs.remove(0);
                    node.toParent=newLeafNode;

                    newLeafNode.childs.add(node);
                    newLeafNode.keys.add(this.keys.remove(0));
                }

                //更新大小
                newLeafNode.size = leftSize;
                this.size = rightSize;

                if (parent==null){//当前叶子节点没有父节点，则新建一个返回

                    //新建一个父节点
                    InnerNode parentInnerNode = new InnerNode();
                    //将新建的节点以及当前节点插入到新建的内部节点当中,并更新 rightKeyForParent
                    K newLeafRightKey = newLeafNode.keys.get(newLeafNode.keys.size() - 1);
                    newLeafNode.rightKeyForParent = newLeafRightKey;
                    parentInnerNode.keys.add(newLeafRightKey);
                    parentInnerNode.childs.add(newLeafNode);

                    K thisLeafRightKey = this.keys.get(this.keys.size() - 1);
                    this.rightKeyForParent = thisLeafRightKey;
                    parentInnerNode.keys.add(thisLeafRightKey);
                    parentInnerNode.childs.add(this);

                    //更新两个节点的父指针
                    newLeafNode.toParent = parentInnerNode;
                    this.toParent = parentInnerNode;


                    parentInnerNode.size = 2;
                    //返回新建的父节点
                    return parentInnerNode;

                }else {//当前叶子节点有父节点，则将当前节点插入到父节点中

                    //插入到调用此方法的内部节点的位置
                    K newRightKey = newLeafNode.keys.get(newLeafNode.keys.size() - 1);
                    newLeafNode.rightKeyForParent = newRightKey;
                    parent.keys.add(position, newRightKey);
                    parent.childs.add(position,newLeafNode);

                    newLeafNode.toParent = parent;

                    //父节点的size加一
                    parent.size++;
                    return null;
                }

            }else {//当前节点不需要分裂
                return null;
            }


        }


        /**
         * 清空该叶子节点
         */
        void clear() {
            this.childs.clear();
            this.keys.clear();
            this.toParent = null;
            this.size = 0;
        }

        /**
         * 在叶子节点中，此方法无意义
         * @param deleteNode 要删除的子节点的指针
         */
        @Override
        void removeChildNode(Node deleteNode) {
            //删除当前节点
            this.toParent=null;
            this.childs = null;
            this.keys = null;
            this.rightKeyForParent=null;
        }

        /**
         * 要删除的节点包含左边界min，不包含右边界max，因此在此处找到的最小节点不能等于max
         */
        @Override
        LinkListNode findMinLinkedNodeWithKey(K minKey, K maxKey) {

            //获取当前叶子节点指向的第一个（即key值最小）的底层链表节点的指针
            LinkListNode currentLinkListNode = this.childs.get(0);

            //跳过前面所有key小于minKey的节点,此后的节点key均大于等于minKey。(当前节点不能为尾节点)
            while (currentLinkListNode.next!=null && currentLinkListNode.key.compareTo(minKey)<0){
                currentLinkListNode=currentLinkListNode.next;
            }

            //找寻所有key大于等于minKey的节点中，key小于maxKey的节点。(当前节点不能为尾节点)
            if (currentLinkListNode.next != null && currentLinkListNode.key.compareTo(maxKey) < 0) {
                return currentLinkListNode;
            } else {
                return null;
            }

        }


        /**
         * 删除指定的底层链表结构，由于底层链表的类与Node不同，故而叶子节点与内部节点不同
         * @param deleteNode 要删除的底层链表节点
         */
        void removeLeafNode(LinkListNode deleteNode) {
            for (int i = 0; i < this.childs.size(); i++) {
                if (this.childs.get(i) == deleteNode) {
                    //彻底移除对应的节点
                    this.keys.remove(i);
                    this.childs.remove(i);

                    this.size--;
                    length--;

                    //如果当前节点没有子节点，则将当前节点从父节点中删除
                    if (this.childs.isEmpty()) {

                        //如果没有父节点，直接返回
                        if (this.toParent == null) {
                            return;
                        }

                        //将当前节点从父节点中删除
                        this.toParent.removeChildNode(this);

                        //删除当前节点
                        this.toParent=null;
                        this.childs = null;
                        this.keys = null;
                        this.rightKeyForParent=null;
                    }
                    return;
                }
            }
        }


    }

    /**
     * 底层的有序链表的节点
     */
    class LinkListNode{
        T item;
        K key;
        Long timestamp;
        LinkListNode next;
        LinkListNode prev;
        LeafNode toParent;

        public LinkListNode(T item, K key, Long timestamp, LinkListNode next, LinkListNode prev, LeafNode toParent) {
            this.item = item;
            this.key = key;
            this.timestamp = timestamp;
            this.next = next;
            this.prev = prev;
            this.toParent = toParent;
        }

        //删除该节点
        public void discard(){
            this.item = null;
            this.key = null;
            this.timestamp = null;
            this.next = null;
            this.prev = null;
            this.toParent = null;
        }

    }



}
