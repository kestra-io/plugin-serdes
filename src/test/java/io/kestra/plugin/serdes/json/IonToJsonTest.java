package io.kestra.plugin.serdes.json;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.utils.IdUtils;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import static io.kestra.core.tenant.TenantService.MAIN_TENANT;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.fail;

@KestraTest
public class IonToJsonTest {

    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private StorageInterface storageInterface;

    @Test
    void test_annotation_conversion() throws Exception {
        String input = """
            {dn:"cn=tony@orga.com,ou=diffusion_list,dc=orga,dc=com",attributes:{description:["Some description 2",base64::"TGlzdGUgZCfDg8KpY2hhbmdlIHN1ciBsZSBzdWl2aSBkZSBsYSBtYXNzZSBzYWxhcmlhbGUgZGUgbCdJVVQ=","Melusine lover as well"],someOtherAttribute:["perhaps 2","perhapsAgain 2"]}}
            """;

        // Expected result when should_keep_annotations==False | or not specified
        String expectation_removed_annot = """
            {"dn":"cn=tony@orga.com,ou=diffusion_list,dc=orga,dc=com","attributes":{"description":["Some description 2","TGlzdGUgZCfDg8KpY2hhbmdlIHN1ciBsZSBzdWl2aSBkZSBsYSBtYXNzZSBzYWxhcmlhbGUgZGUgbCdJVVQ=","Melusine lover as well"],"someOtherAttribute":["perhaps 2","perhapsAgain 2"]}}
            """;

        // Expected result when should_keep_annotations==True
        // String expectation_indicated_annot = """
        //     {"dn":"cn=tony@orga.com,ou=diffusion_list,dc=orga,dc=com","attributes":{"description":["Some description 2",{"ion_annotations":["base64"], "value":"TGlzdGUgZCfDg8KpY2hhbmdlIHN1ciBsZSBzdWl2aSBkZSBsYSBtYXNzZSBzYWxhcmlhbGUgZGUgbCdJVVQ="},"Melusine lover as well"],"someOtherAttribute":["perhaps 2","perhapsAgain 2"]}}
        //     """;

        var runContext = getContext(input);
        var task = IonToJson.builder()
            .from(Property.ofExpression("{{file}}"))
            .build(); // TODO: create two executions, with boolean "should_keep_annotations"
        var output = task.run(runContext);

        assertEquality(expectation_removed_annot, output.getUri()); // by default, annot shouldn't be kept since it's the default behaviour of the ION cookbook https://amazon-ion.github.io/ion-docs/guides/cookbook.html, see "Down-converting to JSON" section
        //assertEquality(expectation_indicated_annot, output.getUri()); // TODO: if should_keep_annotations == true
    }

    private RunContext getContext(String content) {
        Map<String, String> kestraPath = new HashMap<>();
        URI filePath;
        try {
            filePath = storageInterface.put(
                MAIN_TENANT,
                null,
                URI.create("/" + IdUtils.create() + ".ion"),
                new ByteArrayInputStream(content.getBytes())
            );
            kestraPath.put("file", filePath.toString());
        } catch (Exception e) {
            System.err.println(e.getMessage());
            fail("Unable to load input file.");
            return null;
        }
        return runContextFactory.of(ImmutableMap.copyOf(kestraPath));
    }

    private void assertEquality(String expected, URI file) {
        assertThat("Result file should exist", storageInterface.exists(MAIN_TENANT, null, file), is(true));
        try (InputStream streamResult = storageInterface.get(MAIN_TENANT, null, file)) {
            String result = new String(streamResult.readAllBytes(), StandardCharsets.UTF_8).replace("\r\n", "\n");
            System.out.println("Got :\n" + result);
            System.out.println("Expecting :\n" + expected);
            assertThat("Result should match the reference", result.equals(expected));
        } catch (Exception e) {
            System.err.println(e.getMessage());
            fail("Unable to load results files.");
        }
    }
}
