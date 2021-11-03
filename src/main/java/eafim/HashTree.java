package eafim;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;

import static java.lang.Math.max;

public class HashTree implements Serializable {
    static class Node implements Serializable {
        public int numChildren;
        Node[] childrenArray;
        ArrayList<int[]> bucket;

        Node(int numChildren){
            this.numChildren = numChildren;
            childrenArray = new Node[numChildren];
        }
    }

    public int numItemsets;
    public int depth = 7;
    private int hashCode = 10;
    public Node root = new Node(10);

    private int hash(int x){
        return x % hashCode;
    }

    public static HashTree build(int[][] itemsets){
        HashTree result = new HashTree();
        if (itemsets.length > 0) {
            result.depth = itemsets[0].length;
            result.hashCode = max(result.depth, itemsets.length / result.depth);
            result.root = new Node(result.hashCode);
        }
        for(int[] s: itemsets) result.insert(s);
        result.numItemsets = itemsets.length;
        return result;
    }

    public void insert(int[] c){
        insert(this.root, c, 0);
    }

    public void insert(Node u, int[] c, int i){
        if (i == c.length) {
            if (u.bucket == null) u.bucket = new ArrayList<>();
            u.bucket.add(c);
            return;
        }
        if (u.childrenArray == null) u.childrenArray = new Node[hashCode];
        if (u.childrenArray[hash(c[i])] == null) u.childrenArray[hash(c[i])] = new Node(hashCode);
        insert(u.childrenArray[hash(c[i])], c, i + 1);
    }

    public boolean find(Node u, int[] c, int i){
        if (i == c.length) {
            for(int[] t: u.bucket){
                if (Arrays.equals(t, c)) return true;
            }
            return false;
        }
        if (u.childrenArray == null) return false;
        if (u.childrenArray[hash(c[i])] == null) return false;
        return find(u.childrenArray[hash(c[i])], c, i + 1);
    }

    public boolean contains(int[] c){
        return this.find(this.root, c, 0);
    }
}
