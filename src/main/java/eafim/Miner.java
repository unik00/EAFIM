package eafim;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class Miner {
    int minSup;
    JavaRDD<int[]> inputRdd;
    JavaSparkContext sparkContext;

    public Miner(int minSup, JavaSparkContext sc){
        this.minSup = minSup;
        sparkContext = sc;
    }

    public void run(){
        /*
        int k = 1;
        boolean converged = false;
        int[][] previousFrequent = new int[1][];
        while (!converged){
            int[][] currentFrequents = FrequentFinder.findFrequents(inputRdd, previousFrequent, k, minSup);
            if (currentFrequents.length == 0) converged = true;
            else {
                if (currentFrequents.length < previousFrequent.length){
                    inputRdd = InputRDDUpdater.updateInputRDD(inputRdd, currentFrequents, k, minSup);
                }
                previousFrequent = broadcast(currentFrequents);
                k++;
            }
        }
         */
    }
}
