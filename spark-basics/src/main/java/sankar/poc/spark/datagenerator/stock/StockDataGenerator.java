package sankar.poc.spark.datagenerator.stock;

import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import sankar.poc.spark.csvwriter.CsvRecord;
import sankar.poc.spark.csvwriter.CsvWriter;

public class StockDataGenerator {
    
    private static final String DDL_SCHEMA_STRING = "`date` DATE NOT NULL,`index` STRING NOT NULL,`open` FLOAT NOT NULL,`high` FLOAT NOT NULL,`low` FLOAT NOT NULL,`close` FLOAT NOT NULL,`volume` INT";
    private static final String SAMPLE_FILE_DELIMITER = ",";
    private static final String EMPTY_VALUE = "-";
    private static final Function<String, StockRecordRange> mapper = (String lines) -> {
        if (lines == null || lines.isEmpty()) {
            return null;
        }
        String[] columns = lines.split(SAMPLE_FILE_DELIMITER);
        String index = columns[0];
        String open = columns[2];
        String high = columns[3];
        String low = columns[4];
        String close = columns[5];
        String volume = columns[8];
        if (index.equals(EMPTY_VALUE) || open.equals(EMPTY_VALUE) || high.equals(EMPTY_VALUE) || low.equals(EMPTY_VALUE) || close.equals(EMPTY_VALUE) || volume.equals(EMPTY_VALUE)) {
            return null;
        }
        StockRecordRange record = new StockRecordRange(index);
        record.setOpenRange(Float.parseFloat(open));
        record.setVolumeRange(Long.parseLong(volume));
        return record;
    };
    
    public static class StockRecord implements CsvRecord, Serializable {
        private LocalDate date;
        private String index;
        private float open;
        private float high;
        private float low;
        private float close;
        private long volume;
    
        public StockRecord(String index, LocalDate date) {
            Objects.requireNonNull(index, "index is a mandatory parameter");
            Objects.requireNonNull(date, "date is a mandatory parameter");
            this.index =  index;
            this.date = date;
        }
    
        public LocalDate getDate() {
            return this.date;
        }
    
        public void setDate(LocalDate date) {
            this.date = date;
        }
    
        public String getIndex() {
            return this.index;
        }
    
        public void setIndex(String index) {
            this.index = index;
        }
    
        public float getOpen() {
            return this.open;
        }
    
        public void setOpen(float open) {
            this.open = open;
        }
    
        public float getHigh() {
            return this.high;
        }
    
        public void setHigh(float high) {
            this.high = high;
        }
    
        public float getLow() {
            return this.low;
        }
    
        public void setLow(float low) {
            this.low = low;
        }
    
        public float getClose() {
            return this.close;
        }
    
        public void setClose(float close) {
            this.close = close;
        }
    
        public long getVolume() {
            return this.volume;
        }
    
        public void setVolume(long volume) {
            this.volume = volume;
        }
    
        @Override
        public int hashCode() {
            return Objects.hash(date, index);
        }
    
        @Override
        public boolean equals(Object other) {
            if (this == other) return true;
            if (null == other) return false;
            if (other instanceof StockRecord) {
                StockRecord o = (StockRecord)other;
                if (!date.equals(o.getDate()) || !index.equals(o.getIndex())) return false;
            } else {
                return false;
            }
            return true;
        }
    
    
        @Override
        public String toString() {
            return "{" +
                " date='" + getDate() + "'" +
                ", index='" + getIndex() + "'" +
                ", open='" + getOpen() + "'" +
                ", high='" + getHigh() + "'" +
                ", low='" + getLow() + "'" +
                ", close='" + getClose() + "'" +
                ", volume='" + getVolume() + "'" +
                "}";
        }
    
        public String toCsv() {
            return String.format("%s,%s,%.2f,%.2f,%.2f,%.2f,%d", getDate(), getIndex(), getOpen(), getHigh(), getLow(), getClose(), getVolume());
        }

        public static String ddlSchema() {
            return DDL_SCHEMA_STRING;
        }
    }

    public static class StockRecordSupplier implements Supplier<List<StockRecord>> {

        private Set<StockRecordRange> sample;
        private LocalDate currentDate;
        private Random random = new Random();

        public StockRecordSupplier(Path sampleFilePath, LocalDate startFrom) throws IOException {
            Objects.requireNonNull(sampleFilePath, "sample data change file path should not be null");
            Objects.requireNonNull(startFrom, "startFrom date should not be null");
            this.currentDate = startFrom;
            this.sample = readSampleFile(sampleFilePath);
        }

        public StockRecordSupplier(Path sampleFilePath) throws IOException {
            this(sampleFilePath, LocalDate.now());
        }

        private Set<StockRecordRange> readSampleFile(Path sampleFilePath) throws IOException {
            if (!Files.exists(sampleFilePath) || !Files.isReadable(sampleFilePath)) {
                throw new IllegalArgumentException("sampleFile does not exists or not readable");
            }
            Set<StockRecordRange> records;
            List<String> lines = Files.readAllLines(sampleFilePath);
            lines.remove(0);
            try(Stream<String> stream = lines.stream()) {
                records = stream.map(mapper)
                .filter(r -> r != null)
                .collect(Collectors.toSet());
            }
            return records;
        }

        public LocalDate getCurrentDate() {
            return currentDate;
        }

        @Override
        public List<StockRecord> get() {
            List<StockRecord> records = sample.stream()
            .map(c -> {
                StockRecord rec = new StockRecord(c.getIndex(), currentDate);
                Range<Float> range = c.getOpenRange();
                float open = (random.nextFloat() * (range.max() - range.min())) + range.min();
                rec.setOpen(open);
                
                float high = (random.nextFloat() * (((0.3f*open)+open) - open)) + open;
                rec.setHigh(high);

                float low = (random.nextFloat() * (open - (open-(0.3f*open)))) + (open-(0.3f*open));
                rec.setLow(low);

                rec.setClose((random.nextFloat() * (high - low)) + low);

                Range<Long> r = c.getVolumeRange();
                rec.setVolume((long)(random.nextFloat() * (r.max() - r.min())) + r.min());
                return rec;
            })
            .collect(Collectors.toList());
            currentDate = currentDate.plusDays(1);
            return records;
        }   
    }
    
    private static class Range<T> {
        T min;
        T max;

        private Range(T min, T max) {
            this.min = min;
            this.max = max;
        }

        public static <T>Range<T> with(T min, T max) {
            return new Range<>(min, max);
        }

        public T min() {
            return min;
        }

        public T max() {
            return max;
        }
    }

    private static class StockRecordRange {
        private static final float PERCENTAGE = 0.3f;
        private String index;
        private Range<Float> openR;
        private Range<Long> volumeR;

        public StockRecordRange(String index) {
            this.index = index;
        }

        public void setOpenRange(float val) {
            this.openR = Range.with(val - (val * PERCENTAGE), val + (val * PERCENTAGE));
        }

        public void setVolumeRange(Long val) {
            this.volumeR = Range.with((long)(val - (val * PERCENTAGE)), (long)(val + (val * PERCENTAGE)));
        }
        
        public String getIndex() {
            return index;
        }

        public Range<Float> getOpenRange() {
            return openR;
        }

        public Range<Long> getVolumeRange() {
            return volumeR;
        }
    }

    
    /**
     * Produces the employee record as csv files in the given directory
     * @param directory the directory to write the files to
     * @param recordsPerFile no of record per file
     * @param dataFilePrefix the name prefix of the data file generated
     * @param duration the data will be produced once this interval
     * @param timeUnit the timeunit of the interval
     * @throws IOException
     */
    public static void produceCsvFiles(Path directory, int recordsPerFile, String dataFilePrefix, int interval, TimeUnit timeUnit) throws IOException {
        StockRecordSupplier supplier = new StockRecordSupplier(Paths.get("spark-basics/src/main/resources/sample_data.csv"));
        ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();
        AtomicInteger counter = new AtomicInteger(0);
        service.scheduleAtFixedRate(() -> {
            String fileName = String.format("%s_%d.csv", dataFilePrefix, counter.get());
            System.out.printf("generating file %s%n", fileName);
            try {
                Path filePath = CsvWriter.writeAsCsv(supplier.get(), directory, fileName);
                System.out.printf("written file to %s%n", filePath.toString());
                counter.incrementAndGet();
            } catch (IOException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        }, 0, interval, timeUnit);
    }
}
