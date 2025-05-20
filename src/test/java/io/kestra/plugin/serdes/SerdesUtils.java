package io.kestra.plugin.serdes;

import com.devskiller.friendly_id.FriendlyId;
import com.google.common.base.Charsets;
import com.google.common.io.Files;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.tenant.TenantService;
import jakarta.inject.Inject;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Objects;

public class SerdesUtils {
    @Inject
    StorageInterface storageInterface;

    public static String readResource(String file) throws URISyntaxException, IOException {
        return Files.asCharSource(
            SerdesUtils.resourceToFile(file),
            Charsets.UTF_8
        ).read();
    }

    public static File resourceToFile(String file) throws URISyntaxException {
        return new File(Objects.requireNonNull(SerdesUtils.class.getClassLoader()
                .getResource(file))
            .toURI());
    }

    public URI resourceToStorageObject(String file) throws URISyntaxException, IOException {
        return this.resourceToStorageObject(SerdesUtils.resourceToFile(file));
    }

    public URI resourceToStorageObject(File file) throws URISyntaxException, IOException {
        return storageInterface.put(
            TenantService.MAIN_TENANT,
            null,
            new URI("/" + FriendlyId.createFriendlyId()),
            new FileInputStream(file)
        );
    }
}
