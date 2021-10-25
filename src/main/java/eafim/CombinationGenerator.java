package eafim;

import java.util.ArrayList;

public class CombinationGenerator {
    private static void backtrack(ArrayList<int[]> result, int[] transaction, int i, int[] current, int j){
        if (j == current.length) {
            result.add(current.clone());
            return;
        }
        current[j] = transaction[i];
        backtrack(result, transaction, i + 1, current, j + 1);
        if (current.length - j < transaction.length - i) backtrack(result, transaction, i + 1, current, j);
    }

    /** Generate combinations of length k */
    public static int[][] generate(int[] transaction, int k){
        if (transaction.length < k) return new int[0][];
        ArrayList<int[]> result = new ArrayList<>();
        backtrack(result, transaction, 0, new int[k], 0);
        int[][] resultArray = new int[result.size()][];
        for (int i = 0; i < result.size(); i++) {
            resultArray[i] = result.get(i);
        }
        return resultArray;
    }
}
