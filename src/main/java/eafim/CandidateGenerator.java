package eafim;

import java.util.ArrayList;
import java.util.Arrays;

/**
 * While EAFIM doesn't need Apriori Gen procedure, it is better to employ a candidate generator in master node
 * so that we only have to emit indexes of candidate instead of the candidate array itself.
 * Reduce shuffle read and write.
 */
public class CandidateGenerator {
    private static boolean checkPrefix(int[] a, int[] b){
        for(int i = 0; i < a.length - 1; i++) if (a[i] != b[i]) return false;
        return true;
    }

    /**
     * Frequent itemsets must be sorted lexicographically beforehand.
     * (The itemset itself doesn't have to be sorted)
     * @param frequents frequent itemsets
     *                  e.g. int[]{int[]{3, 2, 1}, int[]{3, 2, 4}, int[]{4, 1, 3}, int[]{4, 5, 1}}
     * @return generated candidates
     */
   public static int[][] gen(int[][] frequents){
       ArrayList<int[]> resultRaw = new ArrayList<>();

       for(int i = 0; i < frequents.length - 1; i++){
           for(int j = i + 1; j < frequents.length; j++){
               if (checkPrefix(frequents[i], frequents[j])){
                   int[] joined = Arrays.copyOf(frequents[i], frequents[i].length + 1);
                   joined[joined.length - 1] = frequents[j][frequents[j].length - 1];
                   resultRaw.add(joined);
               }
               else {
                   // this break is necessary
                   break;
               }
           }
       }

       int[][] result = new int[resultRaw.size()][];
       for(int i = 0; i < resultRaw.size(); i++) result[i] = resultRaw.get(i);
       return result;
   }

}
