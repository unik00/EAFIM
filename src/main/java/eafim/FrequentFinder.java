package eafim;

import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;
import utils.ArrayUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class FrequentFinder {
    /**
     * It will increase supports array for any candidates that match transaction
     */
    private static void gen(int[] trans,
                            int k,
                            HashTree candidateTree,
                            int[] supports){
        int[] indexes = CombinationGenerator.generate(trans, k, candidateTree);
        for(int i: indexes){
            supports[i]++;
        }
    }

    private static int[] mergeSupports(int[] a, int[] b){
        int[] c = new int[a.length];
        for(int i = 0; i < a.length; i++) c[i] = a[i] + b[i];
        return c;
    }

    public static int[][] findFrequents(JavaRDD<int[]> inputRdd,
                                        int[][] previousFrequent,
                                        int k,
                                        int minSup,
                                        HashMap<Integer, Integer> singletonOrder){

        int[][] distinctItems = new int[0][];
        if (k == 1){
            List<Integer> items = inputRdd.flatMap(arr -> ArrayUtils.primitiveArrayToArrayList(arr).iterator())
                    .mapToPair(word -> new Tuple2<>(word, 1))
                    .reduceByKey(Integer::sum)
                    .map(pair -> pair._1)
                    .collect();
            distinctItems = new int[items.size()][];
            int i = 0;
            for(Integer item: items) distinctItems[i++] = new int[]{item};
        }

        int[][] candidatesLengthOne = distinctItems; // for k = 1

        JavaRDD<int[]> fm = inputRdd.mapPartitions(
                iterator -> {
                    // EAFIM doesn't broadcast generated candidates. It only broadcasts frequent itemsets,
                    // and then it re-generates candidates in each partition.
                    int[][] generatedCandidates;
                    if (k > 1) generatedCandidates = CandidateGenerator.gen(previousFrequent);
                    else {
                        generatedCandidates = candidatesLengthOne;
                    }
                    HashTree candidateTree = HashTree.build(generatedCandidates);
                    int[] supports = new int[generatedCandidates.length];
                    while (iterator.hasNext()) {
                        int[] trans = iterator.next();
                        gen(trans, k, candidateTree, supports);
                    }
                    ArrayList<int[]> result = new ArrayList<>();
                    result.add(supports);
                    return result.iterator();
                });

        int[] supports = fm.reduce(FrequentFinder::mergeSupports);

        ArrayList<int[]> newFrequents = new ArrayList<>();

        int[][] generatedCandidates = CandidateGenerator.gen(previousFrequent);

        for(int i = 0; i < supports.length; i++) if (supports[i] >= minSup) {
            newFrequents.add(generatedCandidates[i]);
            if (k == 1){
                singletonOrder.put(generatedCandidates[i][0], supports[i]);
            }
        }

        return newFrequents.toArray(new int[0][]);
    }
}
