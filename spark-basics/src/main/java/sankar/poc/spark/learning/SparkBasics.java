package sankar.poc.spark.learning;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import sankar.poc.spark.datagenerator.employee.EmployeeDataGenerator.*;


public class SparkBasics {
    public static void main(String[] args) throws Exception {
        Dataset<Employee> ds = getDs();
        //demo0(ds);
        //demo1(ds);
        demo2(ds);
    }

    private static Dataset<Employee> getDs() {
        SparkSession spark = SparkSession.builder().appName("spark-basics-0").master("local[*]").getOrCreate();
        spark.sparkContext().setLogLevel("WARN");
        EmployeeSupplier supplier = new EmployeeSupplier();
        List<Employee> data = IntStream.range(0, 10).mapToObj(i -> supplier.get()).collect(Collectors.toList());
        Dataset<Employee> ds = spark.createDataset(data, Employee.getEncoder());
        return ds;
    }

    private static void demo0(Dataset<Employee> ds) {
        //create a schema progrmatically
        
        ds.printSchema();
        System.out.println("Full Data(10 rows):");
        ds.show(10);
        System.out.println("");

        //ailas, as, sample, show, take 
        ds = ds.alias("employees").as("employees"); //lias and as are the same. alias could name dataset and columns
        String[] headers = ds.columns();
        System.out.println("Headers in Employee Table: "+ Arrays.asList(headers));
        System.out.println("");
        
        //sampling doesn't create new records it only picks up random recods from the original dataset and create a new one.
        ds = ds.sample(false, 0.5);//with replacement - true, we could have duplicate records, the same record can be sampled twice.
        System.out.println("Below data is with sample");
        ds.show(10);
        System.out.println("");
        

        List<Employee> list = ds.takeAsList(2);
        System.out.println("The first 2 emploee from dataset using take():");
        list.forEach(System.out::println);
        System.out.println("");
        
        System.out.println("Below data is with renamed columns dept --> department");
        Dataset<Row> df = ds.withColumnRenamed("dept", "department");//some operations are only available for dataframe
        df.show(5, true);
    }

    private static void demo1(Dataset<Employee> ds) {
        System.out.println("Full Data(10 rows):");
        ds.show(10);
        System.out.println("");

       
        System.out.println("select with only 3 columns:");
        ds.select("name", "dept", "salary").show(3, false);
        System.out.println("");

        System.out.println("select column with col()");
        ds.select(ds.col("name"), ds.col("salary")).show(5);
        System.out.println("");

        System.out.println("select column with computed column:");
        ds.select(ds.col("name"), ds.col("salary"), ds.col("salary").plus(ds.col("salary").multiply(.10f)).as("incremented_salary")).show(5);
        System.out.println("");

        System.out.println("withColumn Demo:");
        ds.withColumn("salary+10%", ds.col("salary").plus(ds.col("salary").multiply(.10f))).show(5);
        System.out.println("");

        System.out.println("sort by jobGrade demo");
        ds.sort(ds.col("jobGrade").as("job_grade").desc_nulls_last()).show(5);
        System.out.println("");
    }

    private static void demo2(Dataset<Employee> ds) throws AnalysisException {
        System.out.println("Full Data(10 rows):");
        ds.show(10);
        System.out.println("");

        System.out.println("group and aggregate demo");
        ds.select("dept", "salary").groupBy("dept").avg("salary").toDF("department", "avg_salary").show();
        System.out.println("");

        System.out.println("sql demo:");
        ds.createTempView("employee");
        ds.sqlContext().sql("select dept as department, avg(salary) as avg_salary, count(dept) as count from employee group by dept").show();
        System.out.println("");
    }
}
