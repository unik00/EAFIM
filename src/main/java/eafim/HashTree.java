package eafim;

import java.util.ArrayList;
import java.util.HashSet;

import static utils.ArrayUtils.primitiveArrayToArrayList;

public class HashTree {
    // TODO: implement proper HashTree. This is just a quick prototype
    HashSet<ArrayList<Integer>> set = new HashSet<>();

    public static HashTree build(int[][] itemsets){
        HashTree result = new HashTree();
        for(int[] s: itemsets) result.set.add(primitiveArrayToArrayList(s));
        return result;
    }

    public boolean contains(int[] c){
        return this.set.contains(primitiveArrayToArrayList(c));
    }
}
