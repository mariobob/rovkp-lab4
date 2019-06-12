package hr.fer.ztel.rovkp.lab4.util;

import java.util.Iterator;
import java.util.function.Function;

public class Iterables {

    /**
     * Disable instantiation.
     */
    private Iterables() {}

    public static <T> int size(Iterable<T> iterable) {
        int i = 0;
        for (Iterator<T> iter = iterable.iterator(); iter.hasNext(); iter.next()) {
            i++;
        }
        return i;
    }

}
