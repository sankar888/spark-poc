package sankar.poc.spark.datagenerator;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.util.List;

import org.apache.spark.sql.types.FloatType;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.StructType;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import sankar.poc.spark.csvwriter.CsvWriter;
import sankar.poc.spark.datagenerator.stock.StockDataGenerator.StockRecord;
import sankar.poc.spark.datagenerator.stock.StockDataGenerator.StockRecordSupplier;

@RunWith(JUnit4.class)
public class DataGeneratorTest {
    
    public static void main(String[] args) throws IOException {
        //testDataGenerator();
        //testStringFormat();
        //testFileName();
        //testCsvWriter();
        testSparkSchema();
    }

    public static void testDataGenerator() throws IOException {
        StockRecordSupplier gen = new StockRecordSupplier(Paths.get("spark-basics/src/main/resources/sample_data.csv"));
        //day 1
        List<StockRecord> records = gen.get();
        records.forEach(System.out::println);

        //day 2
        records = gen.get();
        System.out.println("--------------------------------------");
        records.forEach(System.out::println);
    }

    public static void testStringFormat() {
        LocalDate today = LocalDate.now();
        String filename = String.format("change_data_%s", today);
        System.out.println(filename);
    }

    public static void testFileName() {
        String fileName = "change_data_2023-05-04.csv";
        Path directory = Paths.get("./spark-basics/src/test/resources");
        Path file = directory.resolve(fileName);
        System.out.println(file);
    }

    public static void testCsvWriter() throws IOException {
        StockRecordSupplier gen = new StockRecordSupplier(Paths.get("spark-basics/src/main/resources/sample_data.csv"));
        Path directory = Paths.get("spark-basics/src/test/resources");
        //day 1
        CsvWriter.writeAsCsv(gen.get(), directory, "sample_data_0.csv");

        //day 2
        CsvWriter.writeAsCsv(gen.get(), directory, "sample_data_1.csv");
    }

    public static void testSparkSchema() {
        StructType schema = new StructType();
        schema = schema.add("name", new StringType(), false); //add will create a new StructType
        schema = schema.add("dept", new StringType(), false);
        schema = schema.add("salary", new FloatType(), false);

        System.out.println(schema.prettyJson());
        System.out.println(schema.treeString(2));
    }
}
