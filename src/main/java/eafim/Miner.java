package eafim;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

public class Miner {
    int minSup;
    JavaRDD<int[]> inputRdd;
    JavaSparkContext sparkContext;

    public Miner(int minSup, JavaSparkContext sc){
        this.minSup = minSup;
        sparkContext = sc;
    }

    public void run(){
        int k = 1;
        boolean converged = false;
        Broadcast<int[][]> previousFrequent = sparkContext.broadcast(new int[1][]);
        while (!converged){
            int[][] currentFrequents = FrequentFinder.findFrequents(inputRdd, previousFrequent, k, minSup);
            if (currentFrequents.length == 0) converged = true;
            else {
                if (currentFrequents.length < previousFrequent.getValue().length){
                    inputRdd = InputRDDUpdater.updateInputRDD(inputRdd, currentFrequents, k, minSup);
                }
                previousFrequent = sparkContext.broadcast(currentFrequents);
                k++;
            }
        }

    }
}
