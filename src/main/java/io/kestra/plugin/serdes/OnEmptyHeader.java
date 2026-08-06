package io.kestra.plugin.serdes;

public enum OnEmptyHeader {
    /**
     * Drop trailing unnamed header columns and any values they hold. This is the default: it removes
     * the empty column that a trailing field separator adds, which otherwise breaks downstream
     * conversions (e.g. to Parquet). An empty name earlier in the header is kept as-is.
     */
    DROP,
    /**
     * Keep every column and give each unnamed one a generated name (col_0, col_1, ...), so no data
     * is lost and downstream conversions still get valid column names.
     */
    RENAME
}
