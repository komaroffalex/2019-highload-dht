package ru.mail.polis.service.cluster;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

public class ClusterNodes {

    private final ArrayList<String> nodes;
    private final String id;

    public ClusterNodes(@NotNull final Set<String> nodes, @NotNull final String id) {
        this.nodes = new ArrayList<>(nodes);
        this.id = id;
    }

    public String keyBelongsTo(@NotNull final ByteBuffer key) {
        final int hashCode = key.hashCode();
        final int node = (hashCode & Integer.MAX_VALUE) % nodes.size();
        return nodes.get(node);
    }

    public Set<String> getNodes() {
        return new HashSet<>(this.nodes);
    }

    public String getId() {
        return this.id;
    }
}
