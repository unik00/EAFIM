package eafim;

import java.util.ArrayList;
import java.util.Arrays;

public class CombinationGenerator {
    /**
     * Generate all combinations of length k that is present in candidateTree.
     * Append indexes of combinations (used to build the candidate tree) instead of itemsets itself.
     */
    private static void backtrack(ArrayList<Integer> result,
                                  int[] transaction,
                                  int i,
                                  int[] current,
                                  int j,
                                  HashTree tree,
                                  HashTree.Node u){
        if (j == current.length) {
            // now we search in u bucket to see if current matches one of the candidates in its bucket
            for(int t = 0; t < u.bucketOfIndexes.size();i ++){
                int[] c = tree.originalItemsets[u.bucketOfIndexes.get(i)];
                if (Arrays.equals(current, c)){
                    result.add(u.bucketOfIndexes.get(i));
                    return;
                }
            }
            return;
        }

        current[j] = transaction[i];

        // pick current[j]
        if (u.childrenArray != null && u.childrenArray[tree.hash(current[j])] != null) {
            backtrack(result, transaction, i + 1, current, j + 1, tree, u.childrenArray[tree.hash(current[j])]);
        }

        // not pick current[j]
        if (current.length - j < transaction.length - i) {
            backtrack(result, transaction, i + 1, current, j, tree, u);
        }
    }

    /**
     * Generate combinations of length k.
     * This only returns indexes of itemsets in the original itemsets which built the candidateTree.
     */
    public static int[] generate(int[] transaction, int k, HashTree candidateTree){
        if (transaction.length < k) return new int[0];
        ArrayList<Integer> result = new ArrayList<>();
        backtrack(result, transaction, 0, new int[k], 0, candidateTree, candidateTree.root);
        int[] resultArray = new int[result.size()];
        for (int i = 0; i < result.size(); i++) {
            resultArray[i] = result.get(i);
        }
        return resultArray;
    }
}
