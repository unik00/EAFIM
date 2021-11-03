package eafim;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import utils.ArrayUtils;

import java.util.Arrays;

public class Miner {
    int minSup;
    JavaRDD<int[]> inputRdd;
    JavaSparkContext sparkContext;
    String inputName;

    public Miner(String inputName, int minSup, JavaSparkContext sc){
        this.inputName = inputName;
        this.minSup = minSup;
        sparkContext = sc;
    }

    public int run(){
        JavaRDD<String> rawTrans = sparkContext.textFile(inputName, 60).cache();
        inputRdd = rawTrans.map(ArrayUtils::stringToSortedArray).cache();

        int k = 1;
        boolean converged = false;
        Broadcast<HashTree> previousFrequent = sparkContext.broadcast(HashTree.build(new int[0][]));

        int totalFrequents = 0;

        while (!converged){
            System.out.println("Mining " + k + " itemsets...");

            System.out.println("Finding frequents...");
            int[][] currentFrequents = FrequentFinder.findFrequents(inputRdd, previousFrequent, k, minSup);
            System.out.println("Finished finding frequents.");
            System.out.println(currentFrequents.length + " frequent itemsets.");
            totalFrequents += currentFrequents.length;
            if (currentFrequents.length == 0) converged = true;
            else {
                HashTree currentFrequentsTree = HashTree.build(currentFrequents);
                Broadcast<HashTree> broadcastTree = sparkContext.broadcast(currentFrequentsTree);
                if (k == 1 || currentFrequentsTree.numItemsets < previousFrequent.getValue().numItemsets) {
                    System.out.println("Updating Input RDD...");
                    InputRDDUpdater.updateInputRDD(this, broadcastTree, k);
                }
                previousFrequent = broadcastTree;
                k++;
            }
            System.out.println("Finished mining " + (k-1) + " itemsets.");
        }
        System.out.println("Total frequent itemsets: " + totalFrequents);
        return totalFrequents;
    }
}
