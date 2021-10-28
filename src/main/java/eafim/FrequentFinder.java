package eafim;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;

import static utils.ArrayUtils.*;

public class FrequentFinder {
    private static Iterator<Tuple2<String, Integer>> gen(int[] trans,
                                                                int k,
                                                                Broadcast<HashTree> previousFrequentsTree){
        HashTree tree = previousFrequentsTree.getValue();
        ArrayList<Tuple2<String, Integer>> result = new ArrayList<>();
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
                result.add(new Tuple2<>(arrayToString(c), 1));
            }
        }
        return result.iterator();
    }

    public static int[][] findFrequents(JavaRDD<int[]> inputRdd, Broadcast<HashTree> previousFrequentTree, int k, int minSup){
        JavaPairRDD<String, Integer> fm = JavaPairRDD.fromJavaRDD(
                inputRdd.flatMap(trans -> gen(trans, k, previousFrequentTree)).cache()
        );

        fm = fm.reduceByKey(Integer::sum).cache();

        System.out.println(k + ", num key before filter: " + fm.count());

        fm = fm.filter(pair -> pair._2 >= minSup).cache();
        System.out.println(k + ", num key after filter: " + fm.count());

        return fm.map(pair -> stringToArray(pair._1)).cache()
                .collect()
                .toArray(new int[0][]);
    }
}
