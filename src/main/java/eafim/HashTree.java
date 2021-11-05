package eafim;

import java.io.Serializable;
import java.util.ArrayList;

public class HashTree implements Serializable {
    static class Node implements Serializable {
        public int numChildren;
        Node[] childrenArray;
        ArrayList<Integer> bucketOfIndexes;
        Node(int numChildren){
            this.numChildren = numChildren;
        }
    }
    public int[][] originalItemsets;
    public int numItemsets;
    private final int hashCode = 15;
    public Node root = new Node(hashCode);

    public int hash(int x){
        return x % hashCode;
    }

    public static HashTree build(int[][] itemsets){
        HashTree result = new HashTree();
        if (itemsets.length > 0) {
            result.root = new Node(result.hashCode);
        }
        result.originalItemsets = itemsets;
        for(int i = 0; i < itemsets.length; i++) result.insert(i);
        result.numItemsets = itemsets.length;
        return result;
    }

    public void insert(int c){
        insert(this.root, c, 0);
    }

    public void insert(Node u, int c, int i){
        if (i == originalItemsets[c].length) {
            if (u.bucketOfIndexes == null) u.bucketOfIndexes = new ArrayList<>();
            u.bucketOfIndexes.add(c);
            return;
        }
        if (u.childrenArray == null) u.childrenArray = new Node[hashCode];
        if (u.childrenArray[hash(originalItemsets[c][i])] == null) {
            u.childrenArray[hash(originalItemsets[c][i])] = new Node(hashCode);
        }
        insert(u.childrenArray[hash(originalItemsets[c][i])], c, i + 1);
    }

    /*
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
