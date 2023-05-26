package sankar.poc.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class HelloWorld {
    public static void main(String[] args) {
        System.out.println("Hello World!");
        String host = "localhost";
        String port = "9000";
        
        SparkSession spark = SparkSession.builder().appName("necc-streaming-app").getOrCreate();
        Dataset<Row> lines = spark
        .readStream()
        .format("socket")
        .option("host", 0)
        .option("port", port)
        .load();
        
    }
}