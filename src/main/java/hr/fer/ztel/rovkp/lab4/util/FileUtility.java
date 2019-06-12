package hr.fer.ztel.rovkp.lab4.util;

import java.nio.file.Files;
import java.nio.file.Path;

public class FileUtility {

    /**
     * Disable instantiation.
     */
    private FileUtility() {}

    /**
     * Checks if the specified <tt>path</tt> exists . This method is designed
     * primarily for doing parameter validation in methods and constructors, as
     * demonstrated below:
     * <blockquote><pre>
     * public Foo(Path path) {
     *     this.path = Helper.requireExists(path);
     * }
     * </pre></blockquote>
     *
     * @param path path to be checked
     * @return <tt>path</tt> if it exists
     * @throws IllegalArgumentException if path does not exist
     */
    public static Path requireExists(Path path) {
        if (!Files.exists(path)) {
            throw new IllegalArgumentException("The system cannot find the path specified: " + path);
        }

        return path;
    }

    /**
     * Checks if the specified <tt>path</tt> exists and is a directory. This
     * method is designed primarily for doing parameter validation in methods
     * and constructors, as demonstrated below:
     * <blockquote><pre>
     * public Foo(Path path) {
     *     this.path = Helper.requireDirectory(path);
     * }
     * </pre></blockquote>
     *
     * @param path path to be checked
     * @return <tt>path</tt> if it exists and is a directory
     * @throws IllegalArgumentException if path does not exist or is not a directory
     */
    public static Path requireDirectory(Path path) {
        requireExists(path);
        if (!Files.isDirectory(path)) {
            throw new IllegalArgumentException("The specified path must be a directory: " + path);
        }

        return path;
    }

}
