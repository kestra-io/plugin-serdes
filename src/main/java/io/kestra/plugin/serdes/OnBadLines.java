package io.kestra.plugin.serdes;

import io.swagger.v3.oas.annotations.media.Schema;

@Schema(title = "How to handle bad lines (e.g., a line with too many fields).")
public enum OnBadLines {
    /**
     * Throw an exception when a bad line is encountered.
     */
    ERROR,
    /**
     * Raise a warning when a bad line is encountered and skip that line.
     */
    WARN,
    /**
     * Skip bad lines without raising or warning when they are encountered.
     */
    SKIP
}