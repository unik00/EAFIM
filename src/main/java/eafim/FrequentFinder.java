package eafim;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

public class FrequentFinder {
    /**
     * Check whether array t is in S
     * TODO: change this to Hash Tree query
     */
    private static boolean isInSet(int[] t, int[][] S){
        for(int[] c: S){
            if (Arrays.equals(c, t)) return true;
        }
        return false;
    }

    private static Iterator<Tuple2<int[], Integer>> gen(int[] trans,
                                                        int k,
                                                        Broadcast<int[][]> previousFrequents){
        ArrayList<Tuple2<int[], Integer>> result = new ArrayList<>();
        int[][] Ct = CombinationGenerator.generate(trans, k);
        for(int[] c: Ct){
            boolean validCandidate = true;
            if (k > 1){
                int[][] H = CombinationGenerator.generate(c, k - 1);
                for(int[] h: H){
                    if (isInSet(h, previousFrequents.getValue())){
                        validCandidate = false;
                        break;
                    }
                }
            }
            if (validCandidate){
                result.add(new Tuple2<>(c, 1));
            }
        }
        return result.iterator();
    }

    public static int[][] findFrequents(JavaRDD<int[]> inputRdd, Broadcast<int[][]> previousFrequent, int k, int minSup){
        JavaPairRDD<int[], Integer> fm = JavaPairRDD.fromJavaRDD(inputRdd.flatMap(trans -> gen(trans, k, previousFrequent)));
        fm.reduceByKey(Integer::sum).filter(pair -> pair._2 >= minSup);
        return fm.map(pair -> pair._1).collect().toArray(new int[0][]);
    }
}
