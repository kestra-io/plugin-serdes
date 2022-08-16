package io.kestra.plugin.serdes.parquet;

import ch.qos.logback.classic.LoggerContext;
import org.slf4j.LoggerFactory;

public abstract class ParquetTools {
    static void handleLogger() {
        // Unable to load native-hadoop library for your platform
        LoggerFactory.getLogger("org.apache.hadoop.util");

        ((LoggerContext) org.slf4j.LoggerFactory.getILoggerFactory())
            .getLoggerList()
            .stream()
            .filter(logger -> logger.getName().startsWith("org.apache.hadoop.util"))
            .forEach(
                logger -> logger.setLevel(ch.qos.logback.classic.Level.ERROR)
            );
    }
}
