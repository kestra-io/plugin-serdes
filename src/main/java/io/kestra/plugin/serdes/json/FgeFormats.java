package io.kestra.plugin.serdes.json;

import java.time.DateTimeException;
import java.time.LocalDate;
import java.util.List;
import java.util.UUID;
import java.util.function.Predicate;
import java.util.regex.Pattern;

import com.networknt.schema.ExecutionContext;
import com.networknt.schema.format.Format;

/**
 * Format checkers that networknt lacks but the java-json-tools validator ships for draft-04.
 * Only {@code matches(ExecutionContext, String)} is overridden so the source compiles against
 * both the Jackson 2 and Jackson 3 flavours of networknt.
 */
final class FgeFormats {
    private static final Pattern HEX = Pattern.compile("[0-9a-fA-F]*");
    private static final Pattern BASE64_ALPHABET = Pattern.compile("[A-Za-z0-9+/]*");
    private static final Pattern MAC = Pattern.compile("[A-Za-z0-9]{2}(?::[A-Za-z0-9]{2}){5}");

    private static final Pattern DATE_TIME = Pattern.compile(
        "(\\d{4})-(\\d{2})-(\\d{2})T(\\d{2}):(\\d{2}):(\\d{2})(?:\\.\\d{1,12})?(?:Z|[+-](\\d{2}):?(\\d{2}))"
    );

    static final List<Format> ALL = List.of(
        format("date-time", FgeFormats::isDateTime),
        format("uuid", FgeFormats::isUuid),
        format("base64", FgeFormats::isBase64),
        hex("md5", 32),
        hex("sha1", 40),
        hex("sha256", 64),
        hex("sha512", 128),
        format("mac", value -> MAC.matcher(value).matches())
    );

    private FgeFormats() {
    }

    private static Format hex(String name, int length) {
        return format(name, value -> value.length() == length && HEX.matcher(value).matches());
    }

    // fge accepts offsets without a colon and up to 12 fraction digits, which networknt's RFC 3339 check rejects.
    private static boolean isDateTime(String value) {
        var matcher = DATE_TIME.matcher(value);
        if (!matcher.matches()) {
            return false;
        }
        try {
            LocalDate.of(Integer.parseInt(matcher.group(1)), Integer.parseInt(matcher.group(2)), Integer.parseInt(matcher.group(3)));
        } catch (DateTimeException e) {
            return false;
        }
        return Integer.parseInt(matcher.group(4)) < 24
            && Integer.parseInt(matcher.group(5)) < 60
            && Integer.parseInt(matcher.group(6)) < 60
            && (matcher.group(7) == null || Integer.parseInt(matcher.group(7)) < 24 && Integer.parseInt(matcher.group(8)) < 60);
    }

    // UUID.fromString is lenient on segment lengths, and fge relies on it.
    private static boolean isUuid(String value) {
        try {
            UUID.fromString(value);
            return true;
        } catch (IllegalArgumentException e) {
            return false;
        }
    }

    // At most two trailing '=' are tolerated, as in fge.
    private static boolean isBase64(String value) {
        if (value.length() % 4 != 0) {
            return false;
        }
        var end = value.length();
        if (value.endsWith("==")) {
            end -= 2;
        } else if (value.endsWith("=")) {
            end -= 1;
        }
        return BASE64_ALPHABET.matcher(value).region(0, end).matches();
    }

    private static Format format(String name, Predicate<String> predicate) {
        return new Format() {
            @Override
            public String getName() {
                return name;
            }

            @Override
            public boolean matches(ExecutionContext executionContext, String value) {
                return predicate.test(value);
            }
        };
    }
}
