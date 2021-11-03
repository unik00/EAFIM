package eafim;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.sources.In;
import scala.Tuple2;

import java.util.*;

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

    public static int[][] findFrequents(JavaRDD<int[]> inputRdd,
                                        Broadcast<HashTree> previousFrequentTree,
                                        int k,
                                        int minSup,
                                        HashMap<Integer, Integer> singletonOrder){
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
