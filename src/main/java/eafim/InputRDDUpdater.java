package eafim;

import org.apache.spark.broadcast.Broadcast;

import java.util.Iterator;
import java.util.TreeSet;

public class InputRDDUpdater {
    private static int[] gen(int[] trans, Broadcast<HashTree> currentFrequents, int k){
        TreeSet<Integer> resultSet = new TreeSet<>();
        int[][] C_f = CombinationGenerator.generate(trans, k);
        for(int[] c: C_f){
            if (currentFrequents.getValue().contains(c)) {
                for(int item: c) resultSet.add(item);
            }
        }
        Iterator<Integer> it = resultSet.iterator();
        int[] result = new int[resultSet.size()];
        int i = 0;
        while (it.hasNext()) result[i++] = it.next();
        return result;
    }

    public static void updateInputRDD(Miner miner, Broadcast<HashTree> currentFrequents, int k){
        miner.inputRdd = miner.inputRdd.map(trans -> gen(trans, currentFrequents, k));
    }
}
