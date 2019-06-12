package hr.fer.ztel.rovkp.lab4.zad1;

import lombok.Builder;
import lombok.Value;

import java.text.SimpleDateFormat;
import java.util.Comparator;
import java.util.Objects;
import java.util.StringJoiner;

/**
 * A polution reading. Contains:
 * <ol>
 *     <li>Ozone</li>
 *     <li>Particullate matter</li>
 *     <li>Carbon monoxide</li>
 *     <li>Sulfure dioxide</li>
 *     <li>Nitgoren dioxide</li>
 *     <li>Longitude</li>
 *     <li>Latitude</li>
 *     <li>Timestamp</li>
 * </ol>
 */
@Value
@Builder
public class PollutionReading implements Comparable<PollutionReading> {

    /** Compares {@code PollutionReading} objects by timestamp. */
    public static final Comparator<PollutionReading> TIMESTAMP_COMPARATOR = Comparator.comparing(PollutionReading::getTimestamp);
    /** Parsing delimiter regex. */
    private static final String PARSE_DELIMITER_REGEX = ",";

    /** Date format for parsing. */
    public static final SimpleDateFormat SIMPLE_DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");

    private final int ozone;
    private final int particullateMatter;
    private final int carbonMonoxide;
    private final int sulfureDioxide;
    private final int nitrogenDioxide;
    private final double longitude;
    private final double latitude;
    private final String timestamp;

    /**
     * Parses the given string as a pollution reading object.
     *
     * Throws an exception if input can not be parsed.
     *
     * @param s string to be parsed
     * @return parsed {@code PollutionReading} object
     * @throws IllegalArgumentException if string can not be parsed
     */
    public static PollutionReading parse(String s) {
        try {
            String[] args = s.split(PARSE_DELIMITER_REGEX);
            return builder()
                    .ozone(Integer.parseInt(args[0]))
                    .particullateMatter(Integer.parseInt(args[1]))
                    .carbonMonoxide(Integer.parseInt(args[2]))
                    .sulfureDioxide(Integer.parseInt(args[3]))
                    .nitrogenDioxide(Integer.parseInt(args[4]))
                    .longitude(Double.parseDouble(args[5]))
                    .latitude(Double.parseDouble(args[6]))
                    .timestamp(args[7])
                    .build();
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid argument: " + s);
        }
    }

    /**
     * Parses the given string as a pollution reading object.
     *
     * Instead of throwing an exception, this method returns <tt>null</tt>
     * if input can not be parsed.
     *
     * @param s string to be parsed
     * @return parsed {@code PollutionReading} object or <tt>null</tt>
     */
    public static PollutionReading parseUnchecked(String s) {
        try {
            return parse(s);
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    /**
     * Returns a CSV string from this pollution reading.
     *
     * @return a CSV string from this pollution reading
     */
    public String toCSV() {
        return new StringJoiner(",")
                .add(Objects.toString(ozone))
                .add(Objects.toString(particullateMatter))
                .add(Objects.toString(carbonMonoxide))
                .add(Objects.toString(sulfureDioxide))
                .add(Objects.toString(nitrogenDioxide))
                .add(Objects.toString(longitude))
                .add(Objects.toString(latitude))
                .add(Objects.toString(timestamp))
                .toString();
    }

    @Override
    public int compareTo(PollutionReading other) {
        return TIMESTAMP_COMPARATOR.compare(this, other);
    }

}
