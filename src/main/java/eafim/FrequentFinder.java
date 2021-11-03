package eafim;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.util.ArrayList;

import static utils.ArrayUtils.listToPrimitiveArray;

public class FrequentFinder {
    private static void gen(int[] trans,
                            int k,
                            Broadcast<HashTree> previousFrequentsTree,
                            HashTree keyValue){
        HashTree tree = previousFrequentsTree.getValue();
        int[][] Ct = CombinationGenerator.generate(trans, k, previousFrequentsTree.getValue());
        //System.out.println("k= " + k +", num combinations for len " + trans.length + ": " + Ct.length);

        for(int[] c: Ct){
            if (keyValue.contains(c)){
                keyValue.insert(c, true);
                continue;
            }
            boolean validCandidate = true;
            if (k > 1){
                int[][] H = CombinationGenerator.generate(c, k - 1);
                for(int[] h: H){
                    if (!tree.contains(h)){
                        validCandidate = false;
                        break;
                    }
                }
            }
            if (validCandidate){
                keyValue.insert(c, true);
            }
        }
    }

    public static int[][] findFrequents(JavaRDD<int[]> inputRdd, Broadcast<HashTree> previousFrequentTree, int k, int minSup){
        JavaPairRDD<ArrayList<Integer>, Integer> fm = JavaPairRDD.fromJavaRDD(
                inputRdd.mapPartitions(
                        iterator -> {
                            HashTree keyValue = new HashTree();
                            while (iterator.hasNext()) {
                                int[] trans = iterator.next();
                                gen(trans, k, previousFrequentTree, keyValue);
                            }
                            ArrayList<Tuple2<ArrayList<Integer>, Integer>> result = keyValue.getAll();
                            return result.iterator();
                        }
                )
        );

        fm = fm.reduceByKey(Integer::sum)
                .filter(pair -> pair._2 >= minSup);

        return fm.map(pair -> listToPrimitiveArray(pair._1))
                .collect()
                .toArray(new int[0][]);
    }
}
