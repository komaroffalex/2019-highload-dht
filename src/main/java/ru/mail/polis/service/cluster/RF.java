package ru.mail.polis.service.cluster;

import com.google.common.base.Splitter;
import one.nio.http.HttpSession;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.List;

import static one.nio.http.Response.BAD_REQUEST;

public class RF {
    private final int ack;
    private final int from;

    public RF(final int ack, final int from) {
        this.ack = ack;
        this.from = from;
    }

    @NotNull
    private static RF of(@NotNull final String value) {
        final String rem = value.replace("=", "");
        final List<String> values = Splitter.on('/').splitToList(rem);
        if (values.size() != 2) {
            throw new IllegalArgumentException("Wrong replica factor:" + value);
        }
        return new RF(Integer.parseInt(values.get(0)), Integer.parseInt(values.get(1)));
    }

    /**
     * Calculate the RF value.
     *
     * @param replicas to define the amount of replicas needed
     * @param session to output responses
     * @param defaultRF to specify the default RF
     * @param clusterSize to specify the size of cluster
     * @return RF value
     */
    public static RF calculateRF(final String replicas, @NotNull final HttpSession session,
                                 final RF defaultRF, final int clusterSize) throws IOException {
        RF rf = null;
        try {
            rf = replicas == null ? defaultRF : RF.of(replicas);
            if (rf.ack < 1 || rf.from < rf.ack || rf.from > clusterSize) {
                throw new IllegalArgumentException("From is too big!");
            }
            return rf;
        } catch (IllegalArgumentException e) {
            session.sendError(BAD_REQUEST, "Wrong RF!");
        }
        return rf;
    }

    public int getFrom() {
        return from;
    }

    public int getAck() {
        return ack;
    }
}
