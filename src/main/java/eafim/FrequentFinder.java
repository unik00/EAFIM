package eafim;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static utils.ArrayUtils.listToPrimitiveArray;

public class FrequentFinder {
    /**
     * Returns a support array
     */
    private static int[] gen(int[] trans,
                            int k,
                            HashTree candidateTree){
        int[][] Ct = CombinationGenerator.generate(trans, k, candidateTree);
        //System.out.println("k= " + k +", num combinations for len " + trans.length + ": " + Ct.length);

        for(int[] c: Ct){

        }

    }

    public static int[][] findFrequents(JavaRDD<int[]> inputRdd,
                                        int[][] previousFrequent,
                                        int k,
                                        int minSup,
                                        HashMap<Integer, Integer> singletonOrder){
        JavaPairRDD<ArrayList<Integer>, Integer> fm = JavaPairRDD.fromJavaRDD(
                inputRdd.mapPartitions(
                        iterator -> {
                            int[][] generatedCandidates = CandidateGenerator.gen(previousFrequent);
                            HashTree candidateTree = HashTree.build(generatedCandidates);
                            while (iterator.hasNext()) {
                                int[] trans = iterator.next();
                                gen(trans, k, candidateTree);
                            }
                            ArrayList<Tuple2<ArrayList<Integer>, Integer>> result = keyValue.getAll();
                            return result.iterator();
                        }
                )
        );

        fm = fm.reduceByKey(Integer::sum)
                .filter(pair -> pair._2 >= minSup);

        if (k == 1){
            List<Tuple2<Integer, Integer>> f1List = fm.map(tuple -> new Tuple2<>(tuple._1.get(0), tuple._2)).collect();
            for (Tuple2<Integer, Integer> integerIntegerTuple2 : f1List) {
                singletonOrder.put(integerIntegerTuple2._1, integerIntegerTuple2._2);
            }
        }

        return fm.map(pair -> listToPrimitiveArray(pair._1))
                .collect()
                .toArray(new int[0][]);
    }
}
