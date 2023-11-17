package sankar.poc.spark.learning;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import sankar.poc.spark.datagenerator.employee.EmployeeDataGenerator.Employee;
import sankar.poc.spark.datagenerator.employee.EmployeeDataGenerator.EmployeeSupplier;

public class SparkTypeSafety {
    
    public static void main(String[] args) {
        Dataset<Employee> ds = getDs();
        demo0(ds);
    }

    private static Dataset<Employee> getDs() {
        SparkSession spark = SparkSession.builder().appName("spark-basics-0").master("local[*]").getOrCreate();
        spark.sparkContext().setLogLevel("WARN");
        EmployeeSupplier supplier = new EmployeeSupplier();
        List<Employee> data = IntStream.range(0, 10).mapToObj(i -> supplier.get()).collect(Collectors.toList());
        data.get(5).setNumericString(null);
        data.get(4).setNumericString("");
        Dataset<Employee> ds = spark.createDataset(data, Employee.getEncoder());
        return ds;
    }

    //Testing the type casting and null handling of spark
    private static void demo0(Dataset<Employee> ds) {
        System.out.println("Schema of employee dataset:");
        ds.schema().printTreeString();
        // root
        // |-- dept: string (nullable = true)
        // |-- designation: string (nullable = true)
        // |-- doj: date (nullable = true)
        // |-- jobGrade: integer (nullable = true)
        // |-- name: string (nullable = true)
        // |-- numericString: string (nullable = true)
        // |-- salary: float (nullable = true)
        ds.show(false);
        ds.createOrReplaceTempView("employee");
        Dataset<Row> df = ds.sparkSession().sql("select distinct(dept) as dept from employee");
        df.schema().printTreeString();
        df.show(false);
    }

}
