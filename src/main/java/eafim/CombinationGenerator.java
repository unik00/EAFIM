package eafim;

import java.util.ArrayList;
import java.util.Arrays;

public class CombinationGenerator {
    /** Previous frequent item-sets can be passed for early pruning */
    private static void backtrack(ArrayList<int[]> result,
                                  int[] transaction,
                                  int i,
                                  int[] current,
                                  int j,
                                  HashTree... previousFrequent){
        if (j == current.length) {
            result.add(current.clone());
            return;
        }

        current[j] = transaction[i];

        if (previousFrequent.length == 0 ||
                current.length < 2 ||
                j < current.length - 2 ||
                previousFrequent[0].contains(Arrays.copyOf(current, current.length - 1))) {
            // pick current[j]

            backtrack(result, transaction, i + 1, current, j + 1, previousFrequent);
        }
//        else {
//            System.out.print("skipped ");
//            System.out.println(Arrays.toString(Arrays.copyOf(current, current.length - 1)));
//            System.out.println(previousFrequent[0].contains(Arrays.copyOf(current, current.length - 1)));
//            System.out.println(j == current.length - 2);
//            System.out.println(previousFrequent[0].set);
//        }

        // not pick current[j]
        if (current.length - j < transaction.length - i) {
            backtrack(result, transaction, i + 1, current, j, previousFrequent);
        }
    }

    /** Generate combinations of length k */
    public static int[][] generate(int[] transaction, int k, HashTree... previousFrequent){
        if (transaction.length < k) return new int[0][];
        ArrayList<int[]> result = new ArrayList<>();
        backtrack(result, transaction, 0, new int[k], 0, previousFrequent);
        int[][] resultArray = new int[result.size()][];
        for (int i = 0; i < result.size(); i++) {
            resultArray[i] = result.get(i);
        }
        return resultArray;
    }
}
