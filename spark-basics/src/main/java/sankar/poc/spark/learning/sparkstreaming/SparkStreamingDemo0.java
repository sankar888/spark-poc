package sankar.poc.spark.learning.sparkstreaming;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;

import sankar.poc.spark.datagenerator.employee.EmployeeDataGenerator;
import sankar.poc.spark.datagenerator.employee.EmployeeDataGenerator.Employee;

public class SparkStreamingDemo0 {

    private static final String SRC_DIR = "C:/Users/sankaraa/work/tmp/spark/input";
    private static final String SRC_TABLE = "employee";
    
    public static void main(String[] args) throws Exception {
        Path directory = Paths.get(SRC_DIR);
        EmployeeDataGenerator.produceCsvFiles(directory, 5, "employee", 15, TimeUnit.SECONDS);
        runSpark();
    }

    private static void runSpark() throws AnalysisException, TimeoutException, StreamingQueryException {
        SparkSession spark = SparkSession.builder()
        .appName("streaming-demo0")
        .master("local[*]")
        .getOrCreate();
        spark.sparkContext().setLogLevel("WARN");

        Dataset<Row> ds = spark.readStream()
        .format("csv")
        .option("path", SRC_DIR)
        .schema(Employee.ddlSchema())
        .option("pathGlobFilter", "employee*.csv")
        .option("cleanSource", "delete")
        .load();

        ds.createTempView(SRC_TABLE);

        Dataset<Row> emp = spark.sql("select dept as department, sum(salary) as total_salary, count(*) as count from employee group by dept order by total_salary desc");
        StreamingQuery query = emp.writeStream()
        .outputMode("complete")
        .format("console")
        .option("checkpointLocation", "C:/Users/sankaraa/work/tmp/spark/streamingdemo0/checkpoint")
        .trigger(Trigger.ProcessingTime(30, TimeUnit.SECONDS))
        .start();

        query.awaitTermination();
    }

}
