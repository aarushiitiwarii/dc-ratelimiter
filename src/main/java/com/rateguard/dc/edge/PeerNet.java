package com.rateguard.dc.edge;

import com.rateguard.dc.common.Log;
import com.rateguard.dc.common.Ports;
import com.rateguard.dc.proto.Empty;
import com.rateguard.dc.proto.PeerGrpc;
import com.rateguard.dc.proto.PeerMsg;
import com.rateguard.dc.proto.SharedBucket;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * Edge-to-edge messaging over the gRPC Peer service. Every message sent to another
 * node is counted, which is how the mutual-exclusion experiments measure overhead.
 */
public final class PeerNet {
    public final String self;
    private volatile List<String> members;
    private final Map<String, PeerGrpc.PeerStub> stubs = new ConcurrentHashMap<>();
    private final Map<String, ManagedChannel> channels = new ConcurrentHashMap<>();
    public final AtomicLong sent = new AtomicLong();
    private volatile Consumer<PeerMsg> handler = m -> {};
    private final Wire wire;

    /** How messages reach another node (gRPC in production, in-memory in tests). */
    public interface Wire { void send(String to, PeerMsg m); }

    public final AtomicLong failed = new AtomicLong();

    /** Delivery result: a failed send is counted and logged (rate-limited), never retried. */
    private StreamObserver<Empty> ack(String to, PeerMsg m) {
        return new StreamObserver<>() {
            public void onNext(Empty e) {}
            public void onCompleted() {}
            public void onError(Throwable t) {
                if (failed.incrementAndGet() % 50 == 1) Log.info("peer send to %s failed (%s): %s", to, m.getBodyCase(), t.getMessage());
            }
        };
    }

    public PeerNet(String self) { this(self, null); }

    public PeerNet(String self, Wire wire) {
        this.self = self;
        this.members = List.of(self);
        this.wire = wire != null ? wire : (to, m) -> io.grpc.Context.ROOT.run(() ->         // not the inbound call's context: it is cancelled when that handler returns
                stub(to).withDeadlineAfter(2, TimeUnit.SECONDS).deliver(m, ack(to, m)));
    }

    public void onMessage(Consumer<PeerMsg> h) { handler = h; }

    public static int idOf(String name) { return Integer.parseInt(name.substring(name.lastIndexOf('-') + 1)); }

    /** Sets the cluster membership (names like "edge-3"), always including this node, ordered by id. */
    public void setMembers(List<String> names) {
        List<String> m = new ArrayList<>(names);
        if (!m.contains(self)) m.add(self);
        m.sort(Comparator.comparingInt(PeerNet::idOf));
        members = List.copyOf(m);
    }

    public List<String> members() { return members; }
    public int size() { return members.size(); }

    public List<String> others() {
        List<String> o = new ArrayList<>(members);
        o.remove(self);
        return o;
    }

    private PeerGrpc.PeerStub stub(String name) {
        return stubs.computeIfAbsent(name, n -> {
            ManagedChannel c = Grpc.newChannelBuilderForAddress(Ports.HOST, Ports.edgePeer(idOf(n)), InsecureChannelCredentials.create()).build();
            channels.put(n, c);
            return PeerGrpc.newStub(c);
        });
    }

    /** One-way message. Messages to self are delivered locally and not counted. */
    public void send(String to, PeerMsg.Builder m) {
        PeerMsg msg = m.setFrom(self).build();
        if (to.equals(self)) { Thread.startVirtualThread(() -> handler.accept(msg)); return; }
        sent.incrementAndGet();
        wire.send(to, msg);
    }

    public void deliver(PeerMsg m) { handler.accept(m); }

    /** Token-bucket state shared through mutual exclusion (travels in messages). */
    static final class Shared {
        double tokens; long ts; long version; boolean init;

        void ensure(com.rateguard.dc.proto.Limit lim, long now) {
            if (!init) { tokens = lim.getCap(); ts = now; init = true; }
        }

        long take(com.rateguard.dc.proto.Limit lim, long want, long now) {
            ensure(lim, now);
            tokens = Math.min(lim.getCap(), tokens + Math.max(0, now - ts) / 1000.0 * lim.getRate());
            ts = now;
            long g = (long) Math.floor(Math.min(tokens, want));
            if (g < 0) g = 0;
            tokens -= g;
            version++;
            return g;
        }

        SharedBucket proto() { return SharedBucket.newBuilder().setTokens(init ? tokens : -1).setTs(ts).setVersion(version).build(); }

        void adoptIfNewer(SharedBucket b) {
            if (b.getTokens() >= 0 && b.getVersion() > version) { tokens = b.getTokens(); ts = b.getTs(); version = b.getVersion(); init = true; }
        }
    }
}
