package eafim;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static utils.ArrayUtils.listToPrimitiveArray;
import static utils.ArrayUtils.primitiveArrayToArrayList;

public class FrequentFinder {
    private static void addToKeyValue(int[] c, HashMap<ArrayList<Integer>, Integer> keyValue){
        ArrayList<Integer> converted = primitiveArrayToArrayList(c);
        int newVal = keyValue.getOrDefault(converted, 0) + 1;
        keyValue.put(converted, newVal);
    }

    private static void gen(int[] trans,
                            int k,
                            Broadcast<HashTree> previousFrequentsTree,
                            HashMap<ArrayList<Integer>, Integer> keyValue){
        HashTree tree = previousFrequentsTree.getValue();
        int[][] Ct = CombinationGenerator.generate(trans, k, previousFrequentsTree.getValue());
        //System.out.println("k= " + k +", num combinations for len " + trans.length + ": " + Ct.length);

        for(int[] c: Ct){
            if (keyValue.containsKey(primitiveArrayToArrayList(c))){
                addToKeyValue(c, keyValue);
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
                addToKeyValue(c, keyValue);
            }
        }
    }

    public static int[][] findFrequents(JavaRDD<int[]> inputRdd, Broadcast<HashTree> previousFrequentTree, int k, int minSup){
        JavaPairRDD<ArrayList<Integer>, Integer> fm = JavaPairRDD.fromJavaRDD(
                inputRdd.mapPartitions(
                        iterator -> {
                            HashMap<ArrayList<Integer>, Integer> keyValue = new HashMap<>();
                            while (iterator.hasNext()) {
                                int[] trans = iterator.next();
                                gen(trans, k, previousFrequentTree, keyValue);
                            }
                            ArrayList<Tuple2<ArrayList<Integer>, Integer>> result = new ArrayList<>();
                            for(Map.Entry<ArrayList<Integer>, Integer> res: keyValue.entrySet()){
                                ArrayList<Integer> key = res.getKey();
                                Integer value = res.getValue();
                                result.add(new Tuple2<>(key, value));
                            }
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
