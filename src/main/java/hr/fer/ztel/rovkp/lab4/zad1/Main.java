package hr.fer.ztel.rovkp.lab4.zad1;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;


/**
 * Gets all readings from pollution files, sorts them by timestamp,
 * converts them to CSV format and writes them to a separate CSV file.
 */
public class Main {

    /** User home directory. For example C:\Users\Bobasti or /home/bobasti */
    private static final String HOME = System.getProperty("user.home");
    /** Directory with pollution data numbered input files. */
    private static final Path INPUT_DIRECTORY = Paths.get(HOME, "Desktop", "pollutionData");
    /** Output file for processed pollution data. */
    private static final Path OUTPUT_FILE = INPUT_DIRECTORY.resolve("polutionData-all.csv");

    /**
     * Program entry point.
     *
     * @param args input directory with pollution files
     *             and output csv file, both optional
     */
    public static void main(String[] args) {
        Path inputDirectory = INPUT_DIRECTORY;
        Path outputFile = OUTPUT_FILE;
        if (args.length == 2) {
            inputDirectory = Paths.get(args[0]);
            outputFile = Paths.get(args[1]);
        }

        PollutionFileReader reader = new PollutionFileReader(inputDirectory);

        try (Stream<PollutionReading> readings = reader.getReadingsFromSensorscopeFiles();
             PrintWriter writer = new PrintWriter(Files.newBufferedWriter(outputFile))) {

            readings.sorted(PollutionReading.TIMESTAMP_COMPARATOR)
                    .map(PollutionReading::toCSV)
                    .forEach(writer::println);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
