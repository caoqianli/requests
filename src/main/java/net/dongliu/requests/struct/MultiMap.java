package net.dongliu.requests.struct;

import java.util.*;

/**
 * map contains a Pair list. same key can have multi values
 *
 * @author Dong Liu dongliu@live.cn
 */
public class MultiMap<K, V, T extends Pair<K, V>> implements Iterable<T> {

    private List<T> pairs;

    /**
     * create empty multi map
     */
    public MultiMap() {
        pairs = new ArrayList<>();
    }

    /**
     * create multi map with pairs
     */
    @SafeVarargs
    public MultiMap(T... pairs) {
        this.pairs = Arrays.asList(pairs);
    }

    /**
     * create multi map with pairs
     */
    public MultiMap(List<T> pairs) {
        this.pairs = pairs;
    }


    /**
     * add one key-value
     */
    public void add(T pair) {
        this.pairs.add(pair);
    }

    /**
     * get first value with key. return null if not found
     */
    public T getFirst(K key) {
        for (T pair : pairs) {
            if (pair.getName().equals(key)) {
                return pair;
            }
        }
        return null;
    }

    /**
     * get values with key. return empty list if not found
     */
    public Collection<T> get(K key) {
        List<T> list = new ArrayList<>();
        for (T pair : pairs) {
            if (pair.getName().equals(key)) {
                list.add(pair);
            }
        }
        return list;
    }

    /**
     * return all data
     */
    public Collection<T> items() {
        return this.pairs;
    }

    public Iterator<T> iterator() {
        return pairs.iterator();
    }

    public int size() {
        return pairs.size();
    }

    public boolean isEmpty() {
        return pairs.isEmpty();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append('{');
        for (int i = 0; i < pairs.size(); i++) {
            T pair = pairs.get(i);
            sb.append(pair.getName()).append(": ").append(pair.getValue());
            if (i != pairs.size() - 1) {
                sb.append(", ");
            }
        }
        sb.append('}');

        return sb.toString();
    }
}
