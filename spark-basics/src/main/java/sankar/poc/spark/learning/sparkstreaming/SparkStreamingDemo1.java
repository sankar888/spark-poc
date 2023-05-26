package sankar.poc.spark.learning.sparkstreaming;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

import sankar.poc.spark.datagenerator.stock.StockDataGenerator;
import sankar.poc.spark.datagenerator.stock.StockDataGenerator.StockRecord;

public class SparkStreamingDemo1 {
    
    private static final String SRC_DIR = "C:/Users/sankaraa/work/tmp/spark/input";
    private static final String SRC_TABLE = "stock";

    public static void main(String[] args) throws Exception {
        Path directory = Paths.get(SRC_DIR);
        StockDataGenerator.produceCsvFiles(directory, -1, "stock_data", 10, TimeUnit.SECONDS);
        runSpark();
    }

    private static void runSpark() throws AnalysisException, TimeoutException, StreamingQueryException {
        SparkSession spark = SparkSession.builder()
        .appName("streaming-demo1")
        .master("local[*]")
        .getOrCreate();
        spark.sparkContext().setLogLevel("WARN");

        Dataset<Row> ds = spark.readStream()
        .format("csv")
        .option("path", SRC_DIR)
        .schema(StockRecord.ddlSchema())
        .option("pathGlobFilter", "stock_data*.csv")
        .option("cleanSource", "delete")
        .load();

        ds.createTempView(SRC_TABLE);

        Dataset<Row> stock = spark.sql("select index, window.start as start, window.end as end, max(high) as 7dayhigh, min(low) as 7daylow, count(*) as count from stock where group by index, window(date, '7 days', '1 days') having index = 'Nifty 500' order by window.end");
        
        StreamingQuery query = stock.writeStream()
        .outputMode("complete")
        .format("console")
        .start();

        query.awaitTermination();
    }
}
