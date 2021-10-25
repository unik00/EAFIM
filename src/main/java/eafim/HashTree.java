package eafim;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;

import static utils.ArrayUtils.primitiveArrayToArrayList;

public class HashTree implements Serializable {
    // TODO: implement proper HashTree. This is just a quick prototype
    HashSet<ArrayList<Integer>> set = new HashSet<>();
    public int numItemsets;

    public static HashTree build(int[][] itemsets){
        HashTree result = new HashTree();
        for(int[] s: itemsets) result.set.add(primitiveArrayToArrayList(s));
        result.numItemsets = itemsets.length;
        return result;
    }

    public boolean contains(int[] c){
        return this.set.contains(primitiveArrayToArrayList(c));
    }
}
