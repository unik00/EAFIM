package eafim;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import utils.ArrayUtils;

import java.util.Arrays;
import java.util.HashMap;

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
        int[][] previousFrequent = new int[0][];

        int totalFrequents = 0;
        HashMap<Integer, Integer> singletonOrder = new HashMap<>();

        while (!converged){
            System.out.println("Mining " + k + " itemsets...");

            System.out.println("Finding frequents...");
            int[][] currentFrequents = FrequentFinder.findFrequents(inputRdd, previousFrequent, k, minSup, singletonOrder);
            System.out.println(currentFrequents.length + " frequent itemsets.");
            //System.out.println(Arrays.deepToString(currentFrequents));
            totalFrequents += currentFrequents.length;
            if (currentFrequents.length == 0) converged = true;
            else {
                if (k == 1 /*|| currentFrequents.length < previousFrequent.length*/) {
                    System.out.println("Updating Input RDD...");
                    InputRDDUpdater.updateInputRDD(this,
                            currentFrequents,
                            k,
                            sparkContext.broadcast(singletonOrder),
                            sparkContext);
                }
                previousFrequent = currentFrequents;
                k++;
            }
            System.out.println("Finished mining " + (k-1) + " itemsets.");
        }
        System.out.println("Total frequent itemsets: " + totalFrequents);
        return totalFrequents;
    }
}
