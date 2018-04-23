package cz.cas.lib.arcstorage.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.io.ByteSource;
import com.google.common.io.Resources;
import cz.cas.lib.arcstorage.domain.DomainObject;
import cz.cas.lib.arcstorage.exception.BadArgument;
import cz.cas.lib.arcstorage.exception.ConfigParserException;
import cz.cas.lib.arcstorage.exception.MissingObject;
import cz.cas.lib.arcstorage.gateway.dto.Checksum;
import org.springframework.aop.framework.Advised;
import org.springframework.aop.support.AopUtils;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.net.URL;
import java.nio.charset.StandardCharsets;
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

    public static <T extends RuntimeException> void gte(Integer n, Integer l, Supplier<T> supplier) {
        if (n < l) {
            throw supplier.get();
        }
    }

    public static <T extends RuntimeException> void gt(BigDecimal n, BigDecimal l, Supplier<T> supplier) {
        if (n.compareTo(l) <= 0) {
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

    public static String strSA(String storageName, String aipId) {
        return strS(storageName) + strA(aipId);
    }

    public static String strSX(String storageName, String xmlId) {
        return strS(storageName) + strX(xmlId);
    }

    public static String strSF(String storageName, String fileId) {
        return strS(storageName) + strF(fileId);
    }

    public static String strA(String aipId) {
        return "Aip: " + aipId + " ";
    }

    public static String strF(String fileId) {
        return "File: " + fileId + " ";
    }

    public static String strX(String xmlId) {
        return "Xml: " + xmlId + " ";
    }

    public static String strS(String storageName) {
        return "Storage: " + storageName + " ";
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
                    return enumeration.valueOf(enumerationClass, value.toUpperCase());
            }
        }
        throw new ConfigParserException(jsonPtrExpr, node.toString(), enumerationClass);
    }

    @FunctionalInterface
    public interface Checked {
        void checked() throws Exception;
    }

    public static <T extends RuntimeException> void checked(Checked method, Supplier<T> supplier) {
        try {
            method.checked();
        } catch (Exception ex) {
            throw supplier.get();
        }
    }

    /**
     * Checks that the checksum has the appropriate format
     *
     * @param checksum
     */
    public static void checkChecksum(Checksum checksum) {
        String hash = checksum.getHash();
        switch (checksum.getType()) {
            case MD5:
                if (!hash.matches("\\p{XDigit}{32}")) {
                    throw new BadArgument("Invalid format of MD5 checksum: " + hash);
                }
                break;
            case SHA512:
                if (!hash.matches("\\p{Alnum}{128}")) {
                    throw new BadArgument("Invalid format of SHA512 checksum: " + hash);
                }
        }
    }

    public static void checkUUID(String id) {
        checked(() -> UUID.fromString(id), () -> new BadArgument(id));
    }

}
