package sankar.poc.spark.csvwriter;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Objects;

public class CsvWriter {
    
    private CsvWriter() {}
    /**
     * Writes the given list of data as csv file to the given directory
     * file name is autogenerted based on data.
     * the format used is change_data_<date>.csv
     */
    public static <R extends CsvRecord> Path writeAsCsv(List<R> records, Path directory, String fileName) throws IOException {
        Objects.requireNonNull(records, "records should not be null");
        Objects.requireNonNull(directory, "directory to write should not be null");
        
        int size = records.size();
        if (size == 0) {
            throw new IllegalArgumentException("cannot write zero records to file");
        }
        Path filePath = directory.resolve(fileName);
        try(BufferedWriter writer = Files.newBufferedWriter(filePath, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {
            for (int i=0; i < size; i++) {
                writer.write(records.get(i).toCsv());
                writer.newLine();
            }
            writer.flush();
        }
        return filePath;
    }
}
