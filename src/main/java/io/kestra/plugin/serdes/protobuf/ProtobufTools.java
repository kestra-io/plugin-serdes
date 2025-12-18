package io.kestra.plugin.serdes.protobuf;

import java.util.HashMap;
import java.util.Map;

import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;

public class ProtobufTools {
    public static Descriptor findMessageDescriptor(
            FileDescriptorSet descriptorSet,
            String fullyQualifiedTypeName) throws Descriptors.DescriptorValidationException {

        // Normalize name (remove leading dot if present)
        String normalizedName = fullyQualifiedTypeName.startsWith(".")
                ? fullyQualifiedTypeName.substring(1)
                : fullyQualifiedTypeName;

        // Build all FileDescriptors with dependencies
        Map<String, FileDescriptor> fileDescriptors = new HashMap<>();
        for (FileDescriptorProto fp : descriptorSet.getFileList()) {
            buildFileDescriptor(fp, descriptorSet, fileDescriptors);
        }

        // Search all message descriptors (including nested)
        for (FileDescriptor fd : fileDescriptors.values()) {
            Descriptor found = findMessageRecursively(fd, normalizedName);
            if (found != null) {
                return found;
            }
        }

        return null;
    }

    private static FileDescriptor buildFileDescriptor(
            FileDescriptorProto fileProto,
            FileDescriptorSet set,
            Map<String, FileDescriptor> cache) throws Descriptors.DescriptorValidationException {

        if (cache.containsKey(fileProto.getName())) {
            return cache.get(fileProto.getName());
        }

        FileDescriptor[] dependencies = fileProto.getDependencyList().stream()
                .map(depName -> {
                    FileDescriptorProto depProto = set.getFileList().stream()
                            .filter(fp -> fp.getName().equals(depName))
                            .findFirst()
                            .orElseThrow(() -> new IllegalStateException("Missing dependency: " + depName));
                    try {
                        return buildFileDescriptor(depProto, set, cache);
                    } catch (Descriptors.DescriptorValidationException e) {
                        throw new RuntimeException(e);
                    }
                })
                .toArray(FileDescriptor[]::new);

        FileDescriptor fd = FileDescriptor.buildFrom(fileProto, dependencies);
        cache.put(fileProto.getName(), fd);
        return fd;
    }

    private static Descriptor findMessageRecursively(
            FileDescriptor fd,
            String fullName) {
        for (Descriptor desc : fd.getMessageTypes()) {
            Descriptor found = findInDescriptor(desc, fullName);
            if (found != null) {
                return found;
            }
        }
        return null;
    }

    private static Descriptor findInDescriptor(
            Descriptor desc,
            String fullName) {
        if (desc.getFullName().equals(fullName)) {
            return desc;
        }
        for (Descriptor nested : desc.getNestedTypes()) {
            Descriptor found = findInDescriptor(nested, fullName);
            if (found != null) {
                return found;
            }
        }
        return null;
    }
}
