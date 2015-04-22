package com.uniclick.databank.mapreduce.redis;
import java.util.Collection;
import java.util.SortedMap;
import java.util.TreeMap;
/**
 * �?��性hash的算法的 操作�?
 * @author ping jie
 *
 */
public class ConsistentHash {

	private final HashFunction hashFunction;
	private final int numberOfReplicas;
	private final SortedMap circle = new TreeMap();
    
	/**
	 * 
	 * @param hashFunction  �?��性hash 的可信算法类
	 * @param numberOfReplicas 虚拟节点的个数，    虚拟节点的作用是为了是key能够更均�?��分配到各实际节点�?
	 * @param nodes 实际节点信息集合
	 */
	public ConsistentHash(HashFunction hashFunction, int numberOfReplicas,String[] nodes) {
		this.hashFunction = hashFunction;
		this.numberOfReplicas = numberOfReplicas;

		for (String node : nodes) {
			add(node);
		}
	}

	/**
	 * 把一个实际节点映射到多个位置上，多个位置的都指向到一个相同的节点�?
	 **/
	public void add(String node) {
		for (int i = 0; i < numberOfReplicas; i++) {
			circle.put(hashFunction.hash(node.toString() + i), node);
		}
	}

	public void remove(String node) {
		for (int i = 0; i < numberOfReplicas; i++) {
			circle.remove(hashFunction.hash(node.toString() + i));
		}
	}

	/**
	 * 根据�?��对象的key获得它的将要存储的节点（如：缓存节点�?
	 **/
	public String get(String key) {
		if (circle.isEmpty()) {
			return null;
		}
		long hash = hashFunction.hash(key);
		if (!circle.containsKey(hash)) {
			SortedMap tailMap = circle.tailMap(hash);//返回比指定key大的部分集合
			hash = (Long) (tailMap.isEmpty() ? circle.firstKey() : tailMap.firstKey());
		}
		return (String) circle.get(hash);
	}

}