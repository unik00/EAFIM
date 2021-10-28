mvn install -DskipTests && spark-submit --class Main --master yarn --packages info.picocli:picocli:4.6.1 --deploy-mode cluster --driver-cores 2 --num-executors 10 --executor-cores 2 --executor-memory 10G --driver-memory 5G --conf spark.executor.heartbeatInterval=100s --conf spark.memory.storageFraction=0.1 --conf spark.memory.fraction=0.1 --conf spark.executor.heartbeatInterval=1000000s --conf spark.network.timeout=10000000s target/EAFIM-1.0-SNAPSHOT.jar -i /user/hdfs/chess.dat -mc 1278
