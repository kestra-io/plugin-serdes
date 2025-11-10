package io.kestra.plugin.serdes.avro;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.kestra.core.validations.DateFormat;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.apache.avro.Conversions;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.data.TimeConversions;
import org.apache.avro.generic.GenericData;
import io.kestra.plugin.serdes.OnBadLines;
import org.apache.avro.util.Utf8;
import org.apache.commons.lang3.StringUtils;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.*;
import java.util.stream.Collectors;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public class AvroConverter {
    @NotNull
    @AvroSchemaValidation
    protected String schema;

    @Builder.Default
    protected final List<String> trueValues = Arrays.asList("t", "true", "enabled", "1", "on", "yes");

    @Builder.Default
    protected final List<String> falseValues = Arrays.asList("f", "false", "disabled", "0", "off", "no", "");

    @Builder.Default
    protected final List<String> nullValues = Arrays.asList(
        "",
        "#N/A",
        "#N/A N/A",
        "#NA",
        "-1.#IND",
        "-1.#QNAN",
        "-NaN",
        "1.#IND",
        "1.#QNAN",
        "NA",
        "n/a",
        "nan",
        "null"
    );

    @Builder.Default
    @DateFormat
    protected final String dateFormat = "yyyy-MM-dd[XXX]";

    @Builder.Default
    @DateFormat
    protected final String timeFormat = "HH:mm[:ss][.SSSSSS][XXX]";

    @Builder.Default
    @DateFormat
    protected final String datetimeFormat = "yyyy-MM-dd'T'HH:mm[:ss][.SSSSSS][XXX]";

    @Builder.Default
    protected final Character decimalSeparator = '.';

    @Builder.Default
    protected Boolean strictSchema = Boolean.FALSE;

    @Builder.Default
    protected Boolean inferAllFields = false;

    @Builder.Default
    protected final String timeZoneId = ZoneId.systemDefault().toString();

    @Builder.Default
    protected final OnBadLines onBadLines = OnBadLines.ERROR;

    private static final GenericData GENERIC_DATA = new GenericData();

    static {
        GENERIC_DATA.addLogicalTypeConversion(new Conversions.UUIDConversion());
        GENERIC_DATA.addLogicalTypeConversion(new Conversions.DecimalConversion());
        GENERIC_DATA.addLogicalTypeConversion(new TimeConversions.DateConversion());
        GENERIC_DATA.addLogicalTypeConversion(new TimeConversions.TimeMicrosConversion());
        GENERIC_DATA.addLogicalTypeConversion(new TimeConversions.TimeMillisConversion());
        GENERIC_DATA.addLogicalTypeConversion(new TimeConversions.TimestampMicrosConversion());
        GENERIC_DATA.addLogicalTypeConversion(new TimeConversions.TimestampMillisConversion());
        GENERIC_DATA.addLogicalTypeConversion(new TimeConversions.LocalTimestampMicrosConversion());
        GENERIC_DATA.addLogicalTypeConversion(new TimeConversions.LocalTimestampMillisConversion());
    }

    public static GenericData genericData() {
        return GENERIC_DATA;
    }

    protected Object getValueFromNameOrAliases(Schema.Field field, Map<String, Object> data) {
        Object value = data.get(field.name());

        if (value != null || field.aliases() == null) {
            return value;
        }

        return field.aliases().stream()
            .map(data::get)
            .filter(Objects::nonNull)
            .findFirst()
            .orElse(null);
    }
    public GenericData.Record fromMap(Schema schema, Map<String, Object> data)
            throws IllegalRowConvertion, IllegalStrictRowConversion {
        return fromMap(schema, data, OnBadLines.ERROR, null);
    }

    public GenericData.Record fromMap(Schema schema, Map<String, Object> data, OnBadLines onBadLines) throws IllegalRowConvertion, IllegalStrictRowConversion {
        return fromMap(schema, data, onBadLines, null);
    }

    public GenericData.Record fromMap(Schema schema, Map<String, Object> data, OnBadLines onBadLines, String parentFieldName) throws IllegalRowConvertion, IllegalStrictRowConversion {

    GenericData.Record record = new GenericData.Record(schema);

    for (Schema.Field field : schema.getFields()) {
        String currentFieldName = parentFieldName != null ? parentFieldName + "." + field.name() : field.name();
        try {
            record.put(field.name(), convert(field.schema(), getValueFromNameOrAliases(field, data), onBadLines, currentFieldName));
        } catch (IllegalCellConversion e) {
            if (onBadLines == OnBadLines.ERROR) {
                throw new IllegalRowConvertion(data, e, field);
            } else if (onBadLines == OnBadLines.WARN) {
                System.err.println("WARN: " + e.getMessage());
                record.put(field.name(), null);
            } else if (onBadLines == OnBadLines.SKIP) {
                record.put(field.name(), null);
            }
        } catch (Exception e) {
            if (onBadLines == OnBadLines.ERROR) {
                throw new IllegalRowConvertion(data, e, field);
            } else if (onBadLines == OnBadLines.WARN) {
                System.err.println("WARN: Conversion error for field '" + field.name() + "': " + e.getMessage());
                record.put(field.name(), null);
            } else {
                record.put(field.name(), null);
            }
        }
    }

    if (this.getStrictSchema() && schema.getFields().size() < data.size()) {
        String message = "Data contains more fields than schema: expected " + schema.getFields().size() + ", got " + data.size();
        if (onBadLines == OnBadLines.ERROR) {
            throw new IllegalStrictRowConversion(schema, schema.getFields().stream().map(Schema.Field::name).collect(Collectors.toList()), data.values());
        } else if (onBadLines == OnBadLines.WARN) {
            System.err.println("WARN: " + message);
        }
    }

    return record;
    }
    public GenericData.Record fromArray(Schema schema, List<? extends Object> data, OnBadLines onBadLines) throws IllegalRowConvertion, IllegalStrictRowConversion {
        HashMap<String, Object> map = new HashMap<>();
        int index = 0;
        for (Schema.Field field : schema.getFields()) {
            Object value = (data.size() > index) ? data.get(index) : null;
            map.put(field.name(), value);
            index++;
        }

        if (this.getStrictSchema() && schema.getFields().size() < data.size()) {
            String message = "Data contains more fields than schema: expected " + schema.getFields().size() + ", got " + data.size();
            if (onBadLines == OnBadLines.ERROR) {
                throw new IllegalStrictRowConversion(schema, map.keySet(), data);
            } else if (onBadLines == OnBadLines.WARN) {
                System.err.println("WARN: " + message);
            }
        }

        return this.fromMap(schema, map, onBadLines, null);
    }

    @SuppressWarnings("unchecked")
    protected Object convert(Schema schema, Object data, OnBadLines onBadLines, String fieldName) throws IllegalCellConversion {

        try {
            if (this.getInferAllFields()) {
                if (data instanceof String && this.contains(this.getNullValues(), (String) data)) {
                    return null;
                } else if (data instanceof String && this.contains(this.getTrueValues(), (String) data)) {
                    return true;
                } else if (data instanceof String && this.contains(this.getFalseValues(), (String) data)) {
                    return false;
                }
            }

            if (schema.getLogicalType() != null && schema.getLogicalType().getName().equals("decimal")) { // logical
                return this.logicalDecimal(schema, data);
            } else if (schema.getLogicalType() != null && schema.getLogicalType().getName().equals("uuid")) {
                return this.logicalUuid(data);
            } else if (schema.getLogicalType() != null && schema.getLogicalType().getName().equals("date")) {
                return this.logicalDate(data);
            } else if (schema.getLogicalType() != null && schema.getLogicalType().getName().equals("time-millis")) {
                return this.logicalTimeMillis(data);
            } else if (schema.getLogicalType() != null && schema.getLogicalType().getName().equals("time-micros")) {
                return this.logicalTimeMicros(data);
            } else if (schema.getLogicalType() != null && schema.getLogicalType().getName().equals("timestamp-millis")) {
                return this.logicalTimestampMillis(data);
            } else if (schema.getLogicalType() != null && schema.getLogicalType().getName().equals("timestamp-micros")) {
                return this.logicalTimestampMicros(data);
            } else if (schema.getLogicalType() != null && schema.getLogicalType().getName().equals("local-timestamp-millis")) {
                return this.logicalTimestampMillis(data).atZone(zoneId()).toLocalDateTime();
            } else if (schema.getLogicalType() != null && schema.getLogicalType().getName().equals("local-timestamp-micros")) {
                return this.logicalTimestampMicros(data).atZone(zoneId()).toLocalDateTime();
            } else if (schema.getType() == Schema.Type.RECORD) { // complex
                return fromMap(schema, (Map<String, Object>) data, onBadLines);
            } else if (schema.getType() == Schema.Type.ARRAY) {
                return this.complexArray(schema, data, onBadLines, fieldName);
            } else if (schema.getType() == Schema.Type.MAP) {
                return this.complexMap(schema, data, onBadLines, fieldName);
            } else if (schema.getType() == Schema.Type.UNION) {
                return this.complexUnion(schema, data, onBadLines, fieldName);
            } else if (schema.getType() == Schema.Type.FIXED) {
                return this.complexFixed(schema, data);
            } else if (schema.getType() == Schema.Type.ENUM) {
                return this.complexEnum(schema, data);
            } else if (schema.getType() == Schema.Type.NULL) { // primitive
                return this.primitiveNull(data);
            } else if (schema.getType() == Schema.Type.INT) {
                return this.primitiveInt(data);
            } else if (schema.getType() == Schema.Type.FLOAT) {
                return this.primitiveFloat(data);
            } else if (schema.getType() == Schema.Type.DOUBLE) {
                return this.primitiveDouble(data);
            } else if (schema.getType() == Schema.Type.LONG) {
                return this.primitiveLong(data);
            } else if (schema.getType() == Schema.Type.BOOLEAN) {
                return this.primitiveBool(data);
            } else if (schema.getType() == Schema.Type.STRING) {
                return this.primitiveString(data);
            } else if (schema.getType() == Schema.Type.BYTES) {
                return this.primitiveBytes(data);
            } else {
                throw new IllegalArgumentException("Invalid schema \"" + schema.getType() + "\"");
            }
        } catch (Throwable e) {
            throw new IllegalCellConversion(schema, data, e);
        }
    }

    protected String convertDecimalSeparator(String value) {
        if (this.getDecimalSeparator() == '.') {
            return value;
        }

        return StringUtils.replaceOnce(value, String.valueOf(this.getDecimalSeparator()), ".");
    }

    @SuppressWarnings("UnpredictableBigDecimalConstructorCall")
    protected BigDecimal logicalDecimal(Schema schema, Object data) {
        int scale = ((LogicalTypes.Decimal) schema.getLogicalType()).getScale();
        int precision = ((LogicalTypes.Decimal) schema.getLogicalType()).getPrecision();
        double multiply = Math.pow(10D, precision - scale * 1D);

        BigDecimal value;

        if (data instanceof String) {
            value = new BigDecimal(convertDecimalSeparator(((String) data)));
        } else if (data instanceof Long) {
            value = BigDecimal.valueOf((long) ((long) data * multiply), scale);
        } else if (data instanceof Integer) {
            value = BigDecimal.valueOf((int) ((int) data * multiply), scale);
        } else if (data instanceof Double) {
            value = new BigDecimal((double) data, new MathContext(precision));
        } else if (data instanceof Float) {
            value = new BigDecimal((float) data, new MathContext(precision));
        } else {
            value = (BigDecimal) data;
        }

        value = value.setScale(scale, RoundingMode.HALF_EVEN);

        return value;
    }

    protected UUID logicalUuid(Object data) {
        if (data instanceof String) {
            return UUID.fromString((String) data);
        } else {
            return (UUID) data;
        }
    }

    protected LocalDate logicalDate(Object data) {
        if (data instanceof String) {
            return LocalDate.parse((String) data, DateTimeFormatter.ofPattern(this.getDateFormat()));
        } else if (data instanceof Date) {
            Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone(this.zoneId()));
            calendar.setTime((Date) data);

            return LocalDate.of(
                calendar.get(Calendar.YEAR),
                calendar.get(Calendar.MONTH),
                calendar.get(Calendar.DAY_OF_MONTH)
            );
        } else if (data instanceof ZonedDateTime) {
            return ((ZonedDateTime) data).toLocalDate();
        } else if (data instanceof OffsetDateTime) {
            return ((OffsetDateTime) data).toLocalDate();
        } else if (data instanceof LocalDateTime) {
            return ((LocalDateTime) data).toLocalDate();
        } else if (data instanceof Instant) {
            return ((Instant) data).atZone(zoneId()).toLocalDate();
        } else {
            return (LocalDate) data;
        }
    }

    protected LocalTime logicalTimeMillis(Object data) {
        if (data instanceof String) {
            return LocalTime.parse((String) data, DateTimeFormatter.ofPattern(this.getTimeFormat()));
        }

        return convertJavaTime(data);
    }

    protected LocalTime logicalTimeMicros(Object data) {
        if (data instanceof String) {
            return LocalTime.parse((String) data, DateTimeFormatter.ofPattern(this.getTimeFormat()));
        }

        return convertJavaTime(data);
    }

    protected LocalTime convertJavaTime(Object data) {
        if (data instanceof OffsetTime) {
            return ((OffsetTime) data).toLocalTime();
        } else {
            return (LocalTime) data;
        }
    }

    protected Instant logicalTimestampMillis(Object data) {
        if (data instanceof String) {
            try {
                return Instant.ofEpochMilli(Long.parseLong((String) data));
            } catch (NumberFormatException ignored) {
            }

            return this.parseDateTime((String) data);
        } else if (data instanceof Long) {
            return Instant.ofEpochMilli((Long) data);
        }

        return convertJavaDateTime(data);
    }

    protected Instant convertJavaDateTime(Object data) {
        if (data instanceof Long) {
            return Instant.ofEpochSecond(0, (Long) data * 1000);
        }

        if (data instanceof LocalDateTime) {
            return ((LocalDateTime) data).atZone(this.zoneId()).toInstant();
        }

        if (data instanceof ZonedDateTime) {
            return ((ZonedDateTime) data).toInstant();
        }

        if (data instanceof OffsetDateTime) {
            return ((OffsetDateTime) data).toInstant();
        }

        return (Instant) data;
    }

    protected Instant parseDateTime(String data) {
        try {
            return ZonedDateTime.parse(data, DateTimeFormatter.ofPattern(this.getDatetimeFormat()))
                .toInstant();
        } catch (DateTimeParseException e) {
            LocalDateTime localDateTime = LocalDateTime.parse(data, DateTimeFormatter.ofPattern(this.getDatetimeFormat()));

            if (this.getTimeZoneId() != null) {
                return localDateTime.atZone(ZoneId.of(this.getTimeZoneId())).toInstant();
            } else {
                return localDateTime.toInstant(ZoneOffset.UTC);
            }
        }
    }

    protected Instant logicalTimestampMicros(Object data) {
        if (data instanceof String) {
            try {
                return Instant.ofEpochSecond(0, Long.parseLong((String) data) * 1000);
            } catch (NumberFormatException ignored) {
            }

            return this.parseDateTime((String) data);
        } else if (data instanceof Long) {
            return Instant.ofEpochSecond(0, (Long) data * 1000);
        }

        return convertJavaDateTime(data);
    }

    @SuppressWarnings("unchecked")
    protected List<Object> complexArray(Schema schema, Object data, OnBadLines onBadLines, String fieldName) throws IllegalCellConversion {
        Schema elementType = schema.getElementType();

        Collection<Object> list = (Collection<Object>) data;
        List<Object> result = new ArrayList<>();

        int index = 0;
        for (Object current : list) {
            String currentFieldName = fieldName + "[" + index + "]";
            result.add(this.convert(elementType, current, onBadLines, currentFieldName));
            index++;
        }

        return result;
    }

    protected Object complexUnion(Schema schema, Object data, OnBadLines onBadLines, String fieldName) {
        for (Schema current : schema.getTypes()) {
            try {
                return this.convert(current, data, onBadLines, fieldName);
            } catch (Exception ignored) {
            }
        }

        throw new IllegalArgumentException("Invalid data for schema \"" + schema.getType() + "\"");
    }

    protected GenericData.Fixed complexFixed(Schema schema, Object data) {
        ByteBuffer value = this.primitiveBytes(data);
        int fixedSize = schema.getFixedSize();

        value.position(0);
        int size = value.remaining();

        if (size != fixedSize) {
            throw new IllegalArgumentException("Invalid length for fixed size, found " + size + ", expexted " + fixedSize);
        }

        return new GenericData.Fixed(schema, value.array());
    }

    @SuppressWarnings("unchecked")
    protected Map<Utf8, Object> complexMap(Schema schema, Object data, OnBadLines onBadLines, String fieldName) throws IllegalCellConversion {
        Schema valueType = schema.getValueType();

        Map<Object, Object> list = (Map<Object, Object>) data;
        Map<Utf8, Object> result = new HashMap<>();

        for (Map.Entry<Object, Object> current : list.entrySet()) {
            String currentFieldName = fieldName + "[" + current.getKey() + "]";
            result.put(
                new Utf8(this.primitiveString(current.getKey()).getBytes()),
                this.convert(valueType, current.getValue(), onBadLines, currentFieldName)
            );
        }

        return result;
    }

    protected GenericData.EnumSymbol complexEnum(Schema schema, Object data) {
        String value = this.primitiveString(data);
        List<String> symbols = schema.getEnumSymbols();

        if (!symbols.contains(value)) {
            throw new IllegalArgumentException("Invalid enum value, found " + value + ", expected " + symbols);
        }

        return new GenericData.EnumSymbol(schema, value);
    }

    protected Integer primitiveNull(Object data) {
        if (data instanceof String && this.contains(this.getNullValues(), (String) data)) {
            return null;
        } else if (data == null) {
            return null;
        } else {
            throw new IllegalArgumentException("Unknown type for null values, found " + data.getClass().getName());
        }
    }

    protected Integer primitiveInt(Object data) {
        if (data instanceof String) {
            return Integer.valueOf((String) data);
        } else {
            return (int) data;
        }
    }

    protected Long primitiveLong(Object data) {
        if (data instanceof String) {
            return Long.valueOf((String) data);
        } else if (data instanceof Integer) {
            return (long) ((int) data);
        } else if (data instanceof BigInteger) {
            return ((BigInteger) data).longValue();
        } else {
            return (long) data;
        }
    }

    protected Float primitiveFloat(Object data) {
        if (data instanceof String) {
            return Float.valueOf(convertDecimalSeparator(((String) data)));
        } else if (data instanceof Integer) {
            return (float) ((int) data);
        } else if (data instanceof Double) {
            return (float) ((double) data);
        } else {
            return (float) data;
        }
    }

    protected Double primitiveDouble(Object data) {
        if (data instanceof String) {
            return Double.valueOf(convertDecimalSeparator(((String) data)));
        } else if (data instanceof Integer) {
            return (double) ((int) data);
        } else if (data instanceof Float) {
            return (double) ((float) data);
        } else {
            return (double) data;
        }
    }

    public Boolean primitiveBool(Object data) {
        if (data instanceof String && this.contains(this.getTrueValues(), (String) data)) {
            return true;
        } else if (data instanceof String && this.contains(this.getFalseValues(), (String) data)) {
            return false;
        } else if (data instanceof Integer && (int) data == 1) {
            return true;
        } else if (data instanceof Integer && (int) data == 0) {
            return false;
        } else {
            return (boolean) data;
        }
    }

    public String primitiveString(Object data) {
        return String.valueOf(data);
    }

    public ByteBuffer primitiveBytes(Object data) {
        return ByteBuffer.wrap(this.primitiveString(data).getBytes());
    }

    protected boolean contains(List<String> list, String data) {
        return list.stream().anyMatch(s -> s.equalsIgnoreCase(data));
    }

    protected ZoneId zoneId() {
        return this.getTimeZoneId() != null ? ZoneId.of(this.getTimeZoneId()) : ZoneOffset.UTC;
    }

    protected static String trimExceptionMessage(Object data) throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        String s = mapper.writeValueAsString(data);

        if (s.length() > 250) {
            s = s.substring(0, 250) + " ...";
        }

        return s;
    }

    @Getter
    public static class IllegalRow extends Exception {
        private final Object data;

        public IllegalRow(Object data, Throwable e) {
            super(e);

            this.data = data;
        }

        @Override
        public String toString() {
            try {
                return super.toString() + " with data [" + trimExceptionMessage(data) + "]";
            } catch (JsonProcessingException e) {
                return super.toString();
            }
        }
    }

    @Getter
    public static class IllegalRowConvertion extends Exception {
        private final Schema.Field field;
        private final Object data;

        public IllegalRowConvertion(Map<String, Object> data, Throwable e, Schema.Field field) {
            super(e);

            this.field = field;
            this.data = data;
        }


        @Override
        public String toString() {
            try {
                return super.toString() + (field != null ? " on field '" + field.name() : "") + "' with data [" + trimExceptionMessage(data) + "]";
            } catch (JsonProcessingException e) {
                return super.toString();
            }
        }
    }

    @Getter
    public static class IllegalStrictRowConversion extends Exception {
        private final Schema schema;
        private final Collection<String> fields;
        private final Collection<?> values;

        public IllegalStrictRowConversion(Schema schema, Collection<String> fields, Collection<?> values) {
            super();

            this.schema = schema;
            this.fields = fields;
            this.values = values;
        }


        @Override
        public String toString() {
            try {
                return "Data contains more fields than schema : on row with " + fields.size() + " Fields [" + trimExceptionMessage(fields)
                    + "], " + values.size() + " Values [" + trimExceptionMessage(values) + "] and Schema [" + schema.toString() + "].";
            } catch (JsonProcessingException e) {
                return super.toString();
            }
        }
    }

    @Getter
    public static class IllegalCellConversion extends Exception {
        private final Object data;
        private final Schema schema;

        public IllegalCellConversion(Schema schema, Object data, Throwable e) {
            super(e);

            this.schema = schema;
            this.data = data;
        }

        @Override
        public String toString() {
            try {
                return super.toString() + " on cols with data [" + trimExceptionMessage(data) + "] and schema [" + schema.toString() + "]";
            } catch (JsonProcessingException e) {
                return super.toString();
            }
        }
    }
}