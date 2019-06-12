package hr.fer.ztel.rovkp.lab4.zad2;


import lombok.Builder;
import lombok.Value;

import java.io.Serializable;

/**
 * US death record. Contains:
 * <ol>
 *     <li>Month of death</li>
 *     <li>Sex</li>
 *     <li>Age</li>
 *     <li>Marital status</li>
 *     <li>Day of week of death</li>
 *     <li>Autopsy</li>
 * </ol>
 */
@Value
@Builder
public class USDeathRecord implements Serializable {
            
    private final int monthOfDeath;
    private final String sex;
    private final int age;
    private final String maritalStatus;
    private final int dayOfWeekOfDeath;
    private final int mannerOfDeath;
    private final String autopsy;
   
    /**
     * Parses the given string as a US death record.
     *
     * Throws an exception if input can not be parsed.
     *
     * @param s string to be parsed
     * @return parsed {@code USDeathRecord} object
     * @throws IllegalArgumentException if string can not be parsed
     */
    public static USDeathRecord parse(String s) {
        try {
            String[] args = s.split(",");
            if (!(args[6].equals("M") || args[6].equals("F"))) {
                throw new IllegalArgumentException("Gender must be either M or F.");
            }
            // Other preconditions

            return builder()
                    .monthOfDeath(Integer.parseInt(args[5]))
                    .sex(args[6])
                    .age(Integer.parseInt(args[8]))
                    .maritalStatus(args[15])
                    .dayOfWeekOfDeath(Integer.parseInt(args[16]))
                    .mannerOfDeath(Integer.parseInt(args[19]))
                    .autopsy(args[21])
                    .build();
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid argument: " + s, e);
        }
    }

    /**
     * Parses the given string as a US death record.
     *
     * Instead of throwing an exception, this method returns <tt>null</tt>
     * if input can not be parsed.
     *
     * @param s string to be parsed
     * @return parsed {@code USDeathRecord} object or <tt>null</tt>
     */
    public static USDeathRecord parseUnchecked(String s) {
        try {
            return parse(s);
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    /**
     * Returns true if record shows a male.
     *
     * @return true if record shows a male
     */
    public boolean isMale() {
        return "M".equals(sex);
    }

    /**
     * Returns true if record shows a female.
     *
     * @return true if record shows a female
     */
    public boolean isFemale() {
        return "F".equals(sex);
    }

    /**
     * Returns true if record shows a married person.
     *
     * @return true if record shows a married person
     */
    public boolean isMarried() {
        return "M".equals(maritalStatus);
    }

    /**
     * Returns true if the corpse was autopsied.
     *
     * @return true if the corpse was autopsied
     */
    public boolean wasAutopsied() {
        return autopsy.equals("Y");
    }

    /**
     * Returns true if the death was accidental.
     *
     * @return true if the death was accidental
     */
    public boolean wasAccidental() {
        return mannerOfDeath == 1;
    }

}