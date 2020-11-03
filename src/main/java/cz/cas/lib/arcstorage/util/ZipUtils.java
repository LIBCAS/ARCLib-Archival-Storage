package cz.cas.lib.arcstorage.util;

import org.apache.commons.io.IOUtils;

import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

/**
 * Custom utils for handling zip archives
 */
public class ZipUtils {
    /**
     * Size of the buffer to read/write data
     */
    private static final int BUFFER_SIZE = 4096;

    /**
     * Extracts a zip entry (file entry), does not close input stream but closes output stream
     *
     * @param zipIn    Input stream from zip file
     * @param filePath Path to output file
     * @throws IOException if something bad happens
     */
    public static void extractFile(ZipInputStream zipIn, Path filePath) throws IOException {
        BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(new FileOutputStream(filePath.toFile()));
        byte[] bytesIn = new byte[BUFFER_SIZE];
        int read;
        while ((read = zipIn.read(bytesIn)) != -1) {
            bufferedOutputStream.write(bytesIn, 0, read);
        }
        bufferedOutputStream.close();
    }

    /**
     * Method compress directory of given name into zip
     *
     * @param sourceDir directory we want to compress into zip
     * @return Byte array of zip file
     * @throws IOException if something bad happens
     */
    public static byte[] compressDirectory(String sourceDir) throws IOException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

        ZipOutputStream zipFile = new ZipOutputStream(byteArrayOutputStream);
        Path srcPath = Paths.get(sourceDir);
        compressDirectoryToZipfile(srcPath.getParent().toString(), srcPath.getFileName().toString(), zipFile);
        IOUtils.close(zipFile);

        return byteArrayOutputStream.toByteArray();
    }

    /**
     * Private method which compress recursively directory into zip file
     *
     * @param rootDir      parent directory of directory we want to compress into zip
     * @param sourceDir    directory we want to compress into zip
     * @param outputStream outputStream into which zip is stored and which is closed
     * @throws IOException if something bad happens
     */
    private static void compressDirectoryToZipfile(String rootDir, String sourceDir, ZipOutputStream outputStream) throws IOException {
        String dir = Paths.get(rootDir, sourceDir).toString();
        for (File file : Objects.requireNonNull(new File(dir).listFiles())) {
            if (file.isDirectory()) {
                compressDirectoryToZipfile(rootDir, Paths.get(sourceDir, file.getName()).toString(), outputStream);
            } else {
                ZipEntry entry = new ZipEntry(Paths.get(sourceDir, file.getName()).toString());
                outputStream.putNextEntry(entry);

                FileInputStream in = new FileInputStream(Paths.get(rootDir, sourceDir, file.getName()).toString());
                IOUtils.copy(in, outputStream);
                IOUtils.close(in);
            }
        }
    }
}
