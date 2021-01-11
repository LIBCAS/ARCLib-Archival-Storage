package cz.cas.lib.arcstorage.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.io.ByteSource;
import com.google.common.io.Resources;
import cz.cas.lib.arcstorage.domain.entity.DomainObject;
import cz.cas.lib.arcstorage.domain.entity.Storage;
import cz.cas.lib.arcstorage.dto.Checksum;
import cz.cas.lib.arcstorage.exception.BadRequestException;
import cz.cas.lib.arcstorage.exception.GeneralException;
import cz.cas.lib.arcstorage.exception.MissingObject;
import cz.cas.lib.arcstorage.service.exception.ConfigParserException;
import cz.cas.lib.arcstorage.storage.StorageService;
import cz.cas.lib.arcstorage.storage.exception.CmdProcessException;
import cz.cas.lib.arcstorage.storage.exception.IOStorageException;
import cz.cas.lib.arcstorage.storage.exception.SshException;
import net.schmizz.sshj.SSHClient;
import net.schmizz.sshj.connection.ConnectionException;
import net.schmizz.sshj.connection.channel.direct.Session;
import net.schmizz.sshj.transport.TransportException;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.aop.framework.Advised;
import org.springframework.aop.support.AopUtils;

import java.io.*;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.*;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class Utils {

    public static <T, U> List<U> map(List<T> objects, Function<T, U> func) {
        if (objects != null) {
            return objects.stream()
                    .map(func)
                    .collect(Collectors.toList());
        } else {
            return null;
        }
    }

    public static <T, U> Set<U> map(Set<T> objects, Function<T, U> func) {
        if (objects != null) {
            return objects.stream()
                    .map(func)
                    .collect(Collectors.toSet());
        } else {
            return null;
        }
    }

    public static <T, U> Map<T, U> asMap(T key, U value) {
        Map<T, U> map = new LinkedHashMap<>();
        map.put(key, value);
        return map;
    }

    public static <T, U> Map<T, U> asMap(T key1, U value1, T key2, U value2) {
        Map<T, U> map = new LinkedHashMap<>();
        map.put(key1, value1);
        map.put(key2, value2);
        return map;
    }

    public static <T> List<T> asList(Collection<T> a) {
        return a.stream().collect(Collectors.toList());
    }

    public static <T> List<T> asList(T... a) {
        return Arrays.asList(a);
    }

    public static <T> T[] asArray(T... a) {
        return a;
    }

    public static <T> List<T> asList(Collection<T> base, T... a) {
        List<T> list = new ArrayList<>(base);
        list.addAll(Arrays.asList(a));

        return list;
    }

    public static <T> Set<T> asSet(Collection<T> a) {
        return new HashSet<>(a);
    }

    public static <T> Set<T> asSet(T... a) {
        return new HashSet<>(Arrays.asList(a));
    }

    public static <T extends RuntimeException> void notNull(Object o, Supplier<T> supplier) {
        if (o == null) {
            throw supplier.get();
        } else if (o instanceof Optional) {
            if (!((Optional) o).isPresent()) {
                throw supplier.get();
            }
        } else if (isProxy(o)) {
            if (unwrap(o) == null) {
                throw supplier.get();
            }
        }
    }

    public static <T extends RuntimeException> void isNull(Object o, Supplier<T> supplier) {
        if (o instanceof Optional) {
            if (((Optional) o).isPresent()) {
                throw supplier.get();
            }
        } else if (isProxy(o)) {
            if (unwrap(o) != null) {
                throw supplier.get();
            }
        } else if (o != null) {
            throw supplier.get();
        }
    }

    public static <U, T extends RuntimeException> void eq(U o1, U o2, Supplier<T> supplier) {
        if (!Objects.equals(o1, o2)) {
            throw supplier.get();
        }
    }

    public static <U, T extends RuntimeException> void in(U o1, Set<U> os2, Supplier<T> supplier) {
        if (!os2.contains(o1)) {
            throw supplier.get();
        }
    }

    public static <U, T extends RuntimeException> void ne(U o1, U o2, Supplier<T> supplier) {
        if (Objects.equals(o1, o2)) {
            throw supplier.get();
        }
    }

    public static <U, T extends RuntimeException> void nin(U o1, Set<U> os2, Supplier<T> supplier) {
        if (os2.contains(o1)) {
            throw supplier.get();
        }
    }

    public static <T extends RuntimeException> void in(Integer n, Integer min, Integer max, Supplier<T> supplier) {
        if (n < min || n > max) {
            throw supplier.get();
        }
    }

    public static <T> void ifPresent(T value, Consumer<T> consumer) {
        if (value != null) {
            consumer.accept(value);
        }
    }

    public static boolean isUuid(String id) {
        try {
            UUID.fromString(id);
            return true;
        } catch (IllegalArgumentException ex) {
            return false;
        }
    }

    public static boolean isMd5(String string) {
        if (string.length() != 32)
            return false;
        try {
            Long.parseLong(string, 16);
            return true;
        } catch (NumberFormatException nfe) {
            return false;
        }
    }

    public static boolean isSha512(String string) {
        if (string.length() != 128)
            return false;
        try {
            Long.parseLong(string, 16);
            return true;
        } catch (NumberFormatException nfe) {
            return false;
        }
    }

    public static boolean isProxy(Object a) {
        return (AopUtils.isAopProxy(a) && a instanceof Advised);
    }

    public static <T> T unwrap(T a) {
        if (isProxy(a)) {
            try {
                return (T) ((Advised) a).getTargetSource().getTarget();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        } else {
            return a;
        }
    }

    public static <T extends DomainObject> List<T> sortByIdList(List<String> ids, Iterable<T> objects) {
        Map<String, T> map = StreamSupport.stream(objects.spliterator(), true)
                .collect(Collectors.toMap(DomainObject::getId, o -> o));

        return ids.stream()
                .map(map::get)
                .filter(o -> o != null)
                .collect(Collectors.toList());
    }

    public static <T> List<T> reverse(List<T> input) {
        List<T> output = new ArrayList<>(input);
        Collections.reverse(output);
        return output;
    }

    public static <T> T[] reverse(T[] array) {
        T[] copy = array.clone();
        Collections.reverse(Arrays.asList(copy));
        return copy;
    }


    public static InputStream resource(String path) throws IOException {
        try {
            URL url = Resources.getResource(path);
            ByteSource source = Resources.asByteSource(url);
            return source.openStream();
        } catch (IllegalArgumentException ex) {
            throw new MissingObject("template", path);
        }
    }

    public static byte[] resourceBytes(String path) throws IOException {
        try {
            URL url = Resources.getResource(path);
            return Resources.toByteArray(url);
        } catch (IllegalArgumentException ex) {
            throw new MissingObject("template", path);
        }
    }

    public static String resourceString(String path) throws IOException {
        try {
            URL url = Resources.getResource(path);
            return Resources.toString(url, StandardCharsets.UTF_8);
        } catch (IllegalArgumentException ex) {
            throw new MissingObject("template", path);
        }
    }

    public static String join(Collection<String> data) {
        if (data == null) {
            return "";
        }

        return data.stream()
                .collect(Collectors.joining(", "));
    }

    public static <T> String join(Collection<T> data, Function<T, String> nameMapper) {
        if (data == null) {
            return "";
        }

        return data.stream()
                .map(nameMapper)
                .collect(Collectors.joining(", "));
    }

    public static <T, U> boolean contains(Collection<T> collection, Function<T, U> mapper, U value) {
        return collection.stream()
                .map(mapper)
                .anyMatch(p -> p.equals(value));
    }

    public static <T, U> T get(Collection<T> collection, Function<T, U> mapper, U value) {
        return collection.stream()
                .filter(t -> Objects.equals(mapper.apply(t), value))
                .findAny()
                .orElse(null);
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    public static boolean isNumber(String text) {
        try {
            Integer.valueOf(text);
            return true;
        } catch (NumberFormatException ex) {
            return false;
        }
    }

    public static String bytesToHexString(byte[] bytes) {
        final StringBuilder builder = new StringBuilder();
        for (byte b : bytes) {
            builder.append(String.format("%02x", b));
        }
        return builder.toString();
    }

    public static <T extends Enum<T>> T parseEnumFromConfig(JsonNode root, String jsonPtrExpr, Class<T> enumerationClass) throws ConfigParserException {
        JsonNode node = root.at(jsonPtrExpr);
        if (!node.isMissingNode()) {
            String value = node.textValue();
            for (Enum enumeration : enumerationClass.getEnumConstants()) {
                if (enumeration.toString().equals(value.toUpperCase()))
                    return Enum.valueOf(enumerationClass, value.toUpperCase());
            }
        }
        throw new ConfigParserException(jsonPtrExpr, node.toString(), enumerationClass);
    }

    /**
     * Checks that the checksum has the appropriate format
     *
     * @param checksum
     * @throws cz.cas.lib.arcstorage.exception.BadRequestException
     */
    public static void checkChecksumFormat(Checksum checksum) throws BadRequestException {
        String hash = checksum.getValue();
        switch (checksum.getType()) {
            case MD5:
                if (!hash.matches("\\p{XDigit}{32}")) {
                    throw new BadRequestException("Invalid format of MD5 checksum: " + hash);
                }
                break;
            case SHA512:
                if (!hash.matches("\\p{Alnum}{128}")) {
                    throw new BadRequestException("Invalid format of SHA512 checksum: " + hash);
                }
        }
    }

    /**
     * Executes process wia command.
     *
     * @param cmd command to be executed
     * @return process return code together with string output
     */
    public static Pair<Integer, List<String>> executeProcessCustomResultHandle(String... cmd) {
        File tmp = null;
        try {
            tmp = File.createTempFile("out", null);
            tmp.deleteOnExit();
            final ProcessBuilder processBuilder = new ProcessBuilder(cmd);
            processBuilder.redirectErrorStream(true).redirectOutput(tmp);
            final Process process = processBuilder.start();
            final int exitCode = process.waitFor();
            return Pair.of(exitCode, Files.readAllLines(tmp.toPath()));
        } catch (InterruptedException | IOException ex) {
            throw new GeneralException("unexpected error while executing process", ex);
        } finally {
            if (tmp != null)
                tmp.delete();
        }
    }

    public static void checkUUID(String id) throws BadRequestException {
        try {
            UUID.fromString(id);
        } catch (Exception e) {
            throw new BadRequestException(id + " is not valid UUID");
        }
    }

    public static void checkIpv4(String ip) throws BadRequestException {
        String PATTERN = "^((0|1\\d?\\d?|2[0-4]?\\d?|25[0-5]?|[3-9]\\d?)\\.){3}(0|1\\d?\\d?|2[0-4]?\\d?|25[0-5]?|[3-9]\\d?)$";
        if (!ip.matches(PATTERN))
            throw new BadRequestException(ip + " is not valid IPv4 address");
    }



    public static byte[] inputStreamToBytes(InputStream ios) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        IOUtils.copy(ios, baos);
        return baos.toByteArray();
    }

    public static List<Storage> servicesToEntities(List<StorageService> storageServices) {
        if (storageServices == null)
            return null;
        return storageServices.stream().map(StorageService::getStorage).collect(Collectors.toList());
    }

    public static LocalDate extractDate(Instant instant) {
        ZonedDateTime zdt = instant.atZone(ZoneId.systemDefault());
        return zdt.toLocalDate();
    }

    public static LocalTime extractTime(Instant instant) {
        ZonedDateTime zdt = instant.atZone(ZoneId.systemDefault());
        return zdt.toLocalTime();
    }

    public static List<String> fetchDataFromRemote(SSHClient ssh, String cmd, Storage storage) throws CmdProcessException, IOStorageException, SshException {
        try (Session s = ssh.startSession()) {
            Session.Command response = s.exec(cmd);
            response.join();
            if (response.getExitStatus() != 0)
                throw new CmdProcessException(cmd, new BufferedReader(new InputStreamReader(response.getErrorStream())).lines().collect(Collectors.toList()), storage);
            try (BufferedReader output = new BufferedReader(new InputStreamReader(response.getInputStream()))) {
                List<String> lines = new ArrayList<>();
                String line = output.readLine();
                if (line == null)
                    return lines;
                while (line != null) {
                    lines.add(line);
                    line = output.readLine();
                }
                return lines;
            } catch (IOException e) {
                throw new IOStorageException(e, storage);
            }
        } catch (ConnectionException | TransportException e) {
            throw new SshException(e, storage);
        }
    }

    public static List<String> fetchDataFromLocal(String cmd, Storage storage) throws CmdProcessException {
        Pair<Integer, List<String>> processResult = executeProcessCustomResultHandle(cmd.split(" "));
        List<String> lines = processResult.getRight();
        if (!processResult.getLeft().equals(0))
            throw new CmdProcessException(cmd, lines, storage);
        return processResult.getRight();
    }
}
