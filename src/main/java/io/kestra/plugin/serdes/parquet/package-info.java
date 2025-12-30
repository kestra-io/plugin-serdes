@PluginSubGroup(
    description = "This sub-group of plugins contains tasks for serialization and deserialization of files in the Apache Parquet format.",
    categories = PluginSubGroup.PluginCategory.TRANSFORMATION,
    categories = {
        PluginSubGroup.PluginCategory.DATA,
        PluginSubGroup.PluginCategory.CORE
    }
)
package io.kestra.plugin.serdes.parquet;

import io.kestra.core.models.annotations.PluginSubGroup;