package eafim;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.broadcast.Broadcast;
import scala.Array;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.TreeSet;

import static utils.ArrayUtils.listToPrimitiveArray;

public class InputRDDUpdater {
    private static Iterator<Tuple2<Long, int[]>> gen(int[] trans,
                                                     long offset,
                                                     Broadcast<HashTree> currentFrequents,
                                                     int k){
        ArrayList<Tuple2<Long, int[]>> result = new ArrayList<>();
        int[][] C_f = CombinationGenerator.generate(trans, k + 1);
        for(int[] c: C_f){
            if (currentFrequents.getValue().contains(c)) result.add(new Tuple2<>(offset, c));
        }
        return result.iterator();
    }

    /**
     * A merge sort can be employed here since elements in every int[] are sorted
     */
    private static int[] mergeValuesDistinct(Tuple2<Long, Iterable<int[]>> keyValues){
        TreeSet<Integer> s = new TreeSet<>();
        for(int[] itemset: keyValues._2){
            for(int item: itemset) s.add(item);
        }
        ArrayList<Integer> result = new ArrayList<>(s);
        return listToPrimitiveArray(result);
    }

    public static void updateInputRDD(Miner miner, Broadcast<HashTree> currentFrequents, int k, int minSup){
        JavaPairRDD<Long, int[]> rdd = JavaPairRDD.fromJavaRDD(
                miner.inputRdd.zipWithIndex().flatMap(pair -> gen(pair._1, pair._2, currentFrequents, k))
        );

        miner.inputRdd = rdd.groupByKey().map(InputRDDUpdater::mergeValuesDistinct);
    }
}
