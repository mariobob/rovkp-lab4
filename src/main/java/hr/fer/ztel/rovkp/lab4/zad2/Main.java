package hr.fer.ztel.rovkp.lab4.zad2;

import hr.fer.ztel.rovkp.lab4.util.Iterables;
import hr.fer.ztel.rovkp.lab4.util.SerializedComparator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Locale;
import java.util.NoSuchElementException;
import java.util.Objects;

import static hr.fer.ztel.rovkp.lab4.util.SerializedComparator.serialize;


// Run on Spark: spark-submit --master local[2] --class hr.fer.ztel.rovkp.lab4.zad2.Main rovkp-lab4-zad2.jar
/**
 * Uses Apache Spark to figure out answers to some questions
 * about death records in the US.
 */
public class Main {

    /** User home directory. For example C:\Users\Bobasti or /home/bobasti */
    private static final String HOME = System.getProperty("user.home");
    /** Death records file. */
    private static final Path INPUT_FILE = Paths.get(HOME, "Desktop", "DeathRecords", "DeathRecords.csv");
    /** Output file for processed data. */
    private static final Path OUTPUT_FILE = INPUT_FILE.resolveSibling("DeathRecords-results.txt");

    /** Apache Spark Java RDD only accepts a serialized comparator. */
    private static final SerializedComparator<Tuple2<Integer, Integer>> TUPLE_COMPARING_INT = serialize((p1, p2) -> Integer.compare(p1._2, p2._2));

    /**
     * Program entry point.
     *
     * @param args input file with names of children
     *             and output csv file, both optional
     */
    public static void main(String[] args) {
        Path inputFile = INPUT_FILE;
        Path outputFile = OUTPUT_FILE;
        if (args.length == 2) {
            inputFile = Paths.get(args[0]);
            outputFile = Paths.get(args[1]);
        }

        SparkConf conf = new SparkConf().setAppName("DeathRecords");

        // Set the master if not already set through the command line
        try {
            conf.get("spark.master");
        } catch (NoSuchElementException ex) {
            conf.setMaster("local");
        }

        JavaSparkContext sc = new JavaSparkContext(conf);

        // Create an RDD from text file lines and filter only valid records
        JavaRDD<USDeathRecord> records = sc.textFile(inputFile.toString())
                .map(USDeathRecord::parseUnchecked)
                .filter(Objects::nonNull);

        // Build the string and write it out
        String result = filterRelevantResults(records);
        System.out.println(result);
//        writeToFile(result, outputFile);
    }

    /**
     * Filters records multiple times and returns a bigass string
     * consisting of an assignment questions and their answers.
     *
     * @param records records to be included in many transformations
     * @return a string consisting of multiple lines
     */
    private static String filterRelevantResults(JavaRDD<USDeathRecord> records) {
        StringBuilder sb = new StringBuilder();

        sb.append("1) How many females older than 40 years old died: ");
        long numFemalesOlderThan40Death = records
                .filter(USDeathRecord::isFemale)
                .filter(record -> record.getAge() > 40)
                .count();
        sb.append(numFemalesOlderThan40Death).append("\n\n");

        sb.append("2) Which month of the year has the most deaths of males younger than 50: ");
        int monthWithMostMaleDeaths = records
                .filter(USDeathRecord::isFemale)
                .filter(r -> r.getAge() < 50)
                .groupBy(USDeathRecord::getMonthOfDeath)
                .aggregateByKey(0, (acc, values) -> Iterables.size(values) + acc, Integer::sum)
                .max(TUPLE_COMPARING_INT)
                ._1();
        sb.append(monthWithMostMaleDeaths).append("\n\n");

        sb.append("3) How many women had their autopsy done after death: ");
        long numFemalesAutopsyDone = records
                .filter(USDeathRecord::isFemale)
                .filter(USDeathRecord::wasAutopsied)
                .count();
        sb.append(numFemalesAutopsyDone).append("\n\n");

        sb.append("4) Number of female deaths between the age of 50 and 65 throughout the days of week: ");
        JavaPairRDD<Integer, Integer> numFemaleDeathsBetween50And65ByDayOfWeek = records
                .filter(USDeathRecord::isFemale)
                .filter(r -> r.getAge() > 50 && r.getAge() < 65)
                .groupBy(USDeathRecord::getDayOfWeekOfDeath)
                .aggregateByKey(0, (acc, values) -> Iterables.size(values) + acc, Integer::sum)
                .sortByKey();
        String numberOfFemaleDeathsByDayOfWeek = numFemaleDeathsBetween50And65ByDayOfWeek
                .map(pair -> String.format("\n%d: %d", pair._1, pair._2))
                .reduce(String::concat);
        sb.append(numberOfFemaleDeathsByDayOfWeek).append("\n\n");

        // Save the number of deaths of females between 50 and 65, for later calculation
        long totalFemaleDeathsBetween50And65 = records
                .filter(USDeathRecord::isFemale)
                .filter(USDeathRecord::isMarried)
                .filter(r -> r.getAge() > 50 && r.getAge() < 65)
                .count();

        sb.append("5) Percentage of married women in female deaths between the age of 50 and 65 throughout the days of week: ");
        String percentageOfNamePerYear = records
                .filter(USDeathRecord::isFemale)
                .filter(USDeathRecord::isMarried)
                .filter(r -> r.getAge() < 65 && r.getAge() > 50)
                .groupBy(USDeathRecord::getDayOfWeekOfDeath)
                .sortByKey()
                .mapToPair(pair -> new Tuple2<>(pair._1, 100.0 * Iterables.size(pair._2) / totalFemaleDeathsBetween50And65))
                .map(pair -> String.format(Locale.US, "\n%d: %.2f%%", pair._1, pair._2))
                .reduce(String::concat);
        sb.append(percentageOfNamePerYear).append("\n\n");

        sb.append("6) Total number of male deaths due to an accident: ");
        long numMaleAccidentalDeaths = records
                .filter(USDeathRecord::isMale)
                .filter(USDeathRecord::wasAccidental)
                .count();
        sb.append(numMaleAccidentalDeaths).append("\n\n");

        sb.append("7) Number of different ages of death: ");
        long numUniqueAges = records
                .map(USDeathRecord::getAge)
                .distinct()
                .count();
        sb.append(numUniqueAges).append("\n\n");

        return sb.toString();
    }

    /**
     * Writes the specified <tt>text</tt> to results output file.
     *
     * @param text text to be written to output file
     */
    private static void writeToFile(String text, Path outputFile) {
        try (PrintWriter writer = new PrintWriter(Files.newBufferedWriter(outputFile, StandardCharsets.UTF_8))) {
            writer.print(text);
        } catch (IOException e) {
            System.err.println("Error writing to file " + outputFile);
            e.printStackTrace();
        }
    }

}
