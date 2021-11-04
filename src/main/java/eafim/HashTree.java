package eafim;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;

public class HashTree implements Serializable {
    static class Node implements Serializable {
        public int numChildren;
        Node[] childrenArray;
        ArrayList<int[]> bucket;
        ArrayList<Integer> counts;

        Node(int numChildren){
            this.numChildren = numChildren;
        }
    }
    public int numItemsets;
    private int hashCode = 100;
    public Node root = new Node(100);

    public int hash(int x){
        return x % hashCode;
    }

    public static HashTree build(int[][] itemsets){
        HashTree result = new HashTree();
        if (itemsets.length > 0) {
            result.hashCode = 100;
            result.root = new Node(result.hashCode);
        }
        for(int[] s: itemsets) result.insert(s, false);
        result.numItemsets = itemsets.length;
        return result;
    }

    public void insert(int[] c, boolean aggregate){
        insert(this.root, c, 0, aggregate);
    }

    public void insert(Node u, int[] c, int i, boolean aggregate){
        if (i == c.length) {
            if (u.bucket == null) u.bucket = new ArrayList<>();
            if (!aggregate) u.bucket.add(c);
            else {
                boolean found = false;
                for(int j = 0; j < u.bucket.size(); j++) {
                    if (Arrays.equals(u.bucket.get(j), c)){
                        found = true;
                        u.counts.set(j, u.counts.get(j) + 1);
                        break;
                    }
                }
                if (!found) {
                    if (u.counts == null) u.counts = new ArrayList<>();
                    u.bucket.add(c);
                    u.counts.add(1);
                }
            }
            return;
        }
        if (u.childrenArray == null) u.childrenArray = new Node[hashCode];
        if (u.childrenArray[hash(c[i])] == null) u.childrenArray[hash(c[i])] = new Node(hashCode);
        insert(u.childrenArray[hash(c[i])], c, i + 1, aggregate);
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

    /*
    private void dfs(Node u, ArrayList<Tuple2<ArrayList<Integer>, Integer>> result){
        if (u.bucket != null){
            for(int i = 0; i < u.bucket.size(); i++){
                result.add(new Tuple2<>(primitiveArrayToArrayList(u.bucket.get(i)), u.counts.get(i)));
            }
        }
        if (u.childrenArray != null){
            for(int i = 0; i < u.childrenArray.length; i++){
                Node v = u.childrenArray[i];
                if (v != null) dfs(v, result);
            }
        }
    }

    public ArrayList<Tuple2<ArrayList<Integer>, Integer>> getAll(){
        ArrayList<Tuple2<ArrayList<Integer>, Integer>> result = new ArrayList<>();
        dfs(root, result);
        return result;
    }
     */
}
