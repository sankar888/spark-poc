package sankar.poc.spark.datagenerator.employee;

import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.hadoop.shaded.org.eclipse.jetty.util.thread.ScheduledExecutorScheduler;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.types.StructType;

import sankar.poc.spark.csvwriter.CsvRecord;
import sankar.poc.spark.csvwriter.CsvWriter;

public class EmployeeDataGenerator {
    public static class Employee implements CsvRecord, Serializable {
        private static final String DDL_SCHEMA = "`name` STRING NOT NULL,`doj` DATE NOT NULL,`designation` STRING,`jobGrade` INT,`dept` STRING,`salary` FLOAT";
        private String name;
        private LocalDate doj;        
        private String designation;
        private int jobGrade;
        private String dept;
        private float salary;

        public String getName() {
            return this.name;
        }
    
        public void setName(String name) {
            this.name = name;
        }
    
        public LocalDate getDoj() {
            return this.doj;
        }
    
        public void setDoj(LocalDate doj) {
            this.doj = doj;
        }
    
        public String getDesignation() {
            return this.designation;
        }
    
        public void setDesignation(String designation) {
            this.designation = designation;
        }
    
        public int getJobGrade() {
            return this.jobGrade;
        }
    
        public void setJobGrade(int jobGrade) {
            this.jobGrade = jobGrade;
        }
    
        public String getDept() {
            return this.dept;
        }
    
        public void setDept(String dept) {
            this.dept = dept;
        }
    
        public float getSalary() {
            return this.salary;
        }
    
        public void setSalary(float salary) {
            this.salary = salary;
        }

        @Override
        public String toString() {
            return "{" +
                " name='" + getName() + "'" +
                ", doj='" + getDoj() + "'" +
                ", designation='" + getDesignation() + "'" +
                ", jobGrade='" + getJobGrade() + "'" +
                ", dept='" + getDept() + "'" +
                ", salary='" + getSalary() + "'" +
                "}";
        }
        
        public static String ddlSchema() {
            return DDL_SCHEMA;
        }

        public static StructType sparkSchema() {
            StructType schema = new StructType();
            schema = schema.add("name", "STRING", false);
            schema = schema.add("doj", "DATE", false);
            schema = schema.add("designation", "STRING", false);
            schema = schema.add("jobGrade", "INT", false);
            schema = schema.add("dept", "STRING", false);
            schema = schema.add("salary", "FLOAT", false);
            return schema;
        }

        public static Encoder<Employee> getEncoder() {
            return ExpressionEncoder.javaBean(Employee.class);
        }

        public String toCsv() {
            return String.format("%s,%s,%s,%s,%s,%.2f", getName(), getDoj(), getDesignation(), getJobGrade(), getDept(), getSalary());
        }
    }

    public static class EmployeeSupplier implements Supplier<Employee> {
        private Random random = new Random();
        private String[] dept = new String[]{"hr", "rnd", "admin", "csuite", "plm"};
        private String[] designation = new String[]{"student", "junior", "junior-1", "senior", "manager", "director"};

        @Override
        public Employee get() {    
            Employee e = new Employee();
            e.setName(getName());
            e.setDoj(getDoj());
            e.setDesignation(getDesignation());
            e.setJobGrade(getJobGrade());
            e.setDept(getDept());
            e.setSalary(getSalary());
            return e;
        }

        private LocalDate getDoj() {
            int year = random.nextInt(2023 - 2000) + 2000;
            int month = random.nextInt(12 - 1) + 1;
            int day = random.nextInt(29 - 1) + 1;
            return LocalDate.of(year, month, day);
        }

        private String getName() {
            return "employee-"+random.nextInt(Integer.MAX_VALUE);
        }

        private String getDesignation() {
            return designation[random.nextInt(6)];
        }

        private int getJobGrade() {
            return random.nextInt(10);
        }

        private String getDept() {
            return dept[random.nextInt(5)];
        }

        private float getSalary() {
            float salary = (random.nextFloat() * (100000 - 50000) + 50000);
            return Math.round(salary * 100.0f) / 100.0f;
        } 
    }

    /**
     * Produces the employee record as csv files in the given directory
     * @param directory the directory to write the files to
     * @param recordsPerFile no of record per file
     * @param dataFilePrefix the name prefix of the data file generated
     * @param duration the data will be produced once this interval
     * @param timeUnit the timeunit of the interval
     */
    public static void produceCsvFiles(Path directory, int recordsPerFile, String dataFilePrefix, int interval, TimeUnit timeUnit) {
        EmployeeSupplier supplier = new EmployeeSupplier();
        ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();
        AtomicInteger counter = new AtomicInteger(0);
        service.scheduleAtFixedRate(() -> {
            String fileName = String.format("%s_%d.csv", dataFilePrefix, counter.get());
            System.out.printf("generating file %s%n", fileName);
            List<Employee> records = IntStream.range(0, recordsPerFile+1).mapToObj(i -> supplier.get()).collect(Collectors.toList());
            try {
                Path filePath = CsvWriter.writeAsCsv(records, directory, fileName);
                System.out.printf("written file to %s%n", filePath.toString());
                counter.incrementAndGet();
            } catch (IOException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        }, 0, interval, timeUnit);
    }
}
