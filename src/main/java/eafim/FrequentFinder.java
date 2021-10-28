package eafim;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;

import static utils.ArrayUtils.listToPrimitiveArray;
import static utils.ArrayUtils.primitiveArrayToArrayList;

public class FrequentFinder {
    private static Iterator<Tuple2<ArrayList<Integer>, Integer>> gen(int[] trans,
                                                                int k,
                                                                Broadcast<HashTree> previousFrequentsTree){
        HashTree tree = previousFrequentsTree.getValue();
        ArrayList<Tuple2<ArrayList<Integer>, Integer>> result = new ArrayList<>();
        int[][] Ct = CombinationGenerator.generate(trans, k);

        for(int[] c: Ct){
            boolean validCandidate = true;
            if (k > 1){
                int[][] H = CombinationGenerator.generate(c, k - 1);
                for(int[] h: H){
                    // TODO: add cached check here
                    if (!tree.contains(h)){
                        validCandidate = false;
                        break;
                    }
                }
            }
            if (validCandidate){
                // convert to ArrayList for reduceByKey (cannot apply reduceByKey on primitive array)
                result.add(new Tuple2<>(primitiveArrayToArrayList(c), 1));
            }
        }
        return result.iterator();
    }

    public static int[][] findFrequents(JavaRDD<int[]> inputRdd, Broadcast<HashTree> previousFrequentTree, int k, int minSup){
        JavaPairRDD<ArrayList<Integer>, Integer> fm = JavaPairRDD.fromJavaRDD(
                inputRdd.flatMap(trans -> gen(trans, k, previousFrequentTree)).cache()
        );

        fm = fm.reduceByKey(Integer::sum)
                .filter(pair -> pair._2 >= minSup).cache();

        return fm.map(pair -> listToPrimitiveArray(pair._1)).cache()
                .collect()
                .toArray(new int[0][]);
    }
}
