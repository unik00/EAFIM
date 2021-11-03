package eafim;

import org.apache.spark.broadcast.Broadcast;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;

import static utils.ArrayUtils.integerToPrimitive;

public class InputRDDUpdater {
    private static int ascendingSupport(Integer a, Integer b, Broadcast<HashMap<Integer, Integer>> order){
        return order.value().get(a).compareTo(order.value().get(b));
    }

    private static int[] gen(int[] trans,
                             Broadcast<HashTree> currentFrequents,
                             int k,
                             Broadcast<HashMap<Integer, Integer>> singletonOrder){
        HashSet<Integer> resultSet = new HashSet<>();
        int[][] C_f = CombinationGenerator.generate(trans, k);
        for(int[] c: C_f){
            if (currentFrequents.getValue().contains(c)) {
                for(int item: c) {
                    resultSet.add(item);
                }
            }
        }
        Iterator<Integer> it = resultSet.iterator();
        Integer[] result = new Integer[resultSet.size()];
        int i = 0;
        while (it.hasNext()) result[i++] = it.next();
        Arrays.sort(result, (x, y) -> ascendingSupport(x, y, singletonOrder));
        return integerToPrimitive(result);
    }

    public static void updateInputRDD(Miner miner, Broadcast<HashTree> currentFrequents, int k,
                                      Broadcast<HashMap<Integer, Integer>> singletonOrder){
        miner.inputRdd = miner.inputRdd.map(trans -> gen(trans, currentFrequents, k, singletonOrder)).cache();
    }
}
