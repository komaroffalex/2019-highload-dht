package ru.mail.polis.service.cluster;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class ClusterNodes {

    private final List<String> nodes;
    private final String id;

    public ClusterNodes(@NotNull final Set<String> nodes, @NotNull final String id) {
        this.nodes = new ArrayList<>(nodes);
        this.id = id;
    }

    /**
     * Check the cluster to which the key belongs to.
     *
     * @param key to search for a specified key
     * @return id of the cluster node to which the key belongs
     */
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

    /**
     * Get the clusters ids where the replicas will be created.
     *
     * @param count the amount of replicas
     * @param key key id
     * @return ids of the clusters to create replicas
     */
    public String[] replicas(final int count, @NotNull final ByteBuffer key) {
        final String[] res = new String[count];
        int index = (key.hashCode() & Integer.MAX_VALUE) % nodes.size();
        for (int j = 0; j < count; j++) {
            res[j] = nodes.get(index);
            index = (index + 1) % nodes.size();
        }
        return res;
    }
}
