package eafim;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;

import static utils.ArrayUtils.integerToPrimitive;

public class InputRDDUpdater {
    public static int ascendingSupport(Integer a, Integer b, HashMap<Integer, Integer> order){
        int tmp = order.get(a).compareTo(order.get(b));
        if (tmp != 0) return tmp;
        else return a.compareTo(b);
    }

    private static int[] gen(int[] trans,
                             Broadcast<int[][]> currentFrequentsBC,
                             Broadcast<HashTree> frequentsTree,
                             int k,
                             Broadcast<HashMap<Integer, Integer>> singletonOrder){
        HashSet<Integer> resultSet = new HashSet<>();
        int[] indexes = CombinationGenerator.generate(trans, k, frequentsTree.getValue());

        int[][] currentFrequents = currentFrequentsBC.getValue();
        for(int i: indexes){
            for(int item: currentFrequents[i]) {
                resultSet.add(item);
            }
        }
        Iterator<Integer> it = resultSet.iterator();
        Integer[] result = new Integer[resultSet.size()];
        int i = 0;
        while (it.hasNext()) result[i++] = it.next();
        Arrays.sort(result, (x, y) -> ascendingSupport(x, y, singletonOrder.getValue()));
        //System.out.println("input rdd updater: " + Arrays.toString(result));
        return integerToPrimitive(result);
    }

    public static void updateInputRDD(Miner miner,
                                      int[][] currentFrequents,
                                      int k,
                                      Broadcast<HashMap<Integer, Integer>> singletonOrder,
                                      JavaSparkContext sparkContext){
        Broadcast<HashTree> frequentsTree = sparkContext.broadcast(HashTree.build(currentFrequents));
        Broadcast<int[][]> currentFrequentsBC = sparkContext.broadcast(currentFrequents);
        miner.inputRdd.unpersist();
        miner.inputRdd = miner.inputRdd.map(trans -> gen(trans,
                currentFrequentsBC,
                frequentsTree,
                k,
                singletonOrder)).cache();
    }
}
