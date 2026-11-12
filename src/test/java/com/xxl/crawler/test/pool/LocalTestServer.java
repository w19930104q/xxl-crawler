package com.xxl.crawler.test.pool;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * local http server for crawler regression tests.
 *
 * <p>per-path programmable behavior:
 * <ul>
 *     <li>{@code failUntil} first N requests respond 500 (transient failure), afterwards 200;</li>
 *     <li>{@code alwaysFail} responds 500 on every request (final failure);</li>
 *     <li>{@code slowMillis} blocks per request to simulate in-flight pages;</li>
 *     <li>body may contain absolute links for spread/dedup coverage.</li>
 * </ul>
 */
public class LocalTestServer implements AutoCloseable {

    private final HttpServer server;
    private final String baseUrl;

    /** path -> request counter */
    private final Map<String, AtomicInteger> hitCounters = new ConcurrentHashMap<>();

    /** path -> number of leading requests that must fail (transient failure threshold) */
    private final Map<String, Integer> failUntil = new ConcurrentHashMap<>();

    /** path -> always fail */
    private final Map<String, Boolean> alwaysFail = new ConcurrentHashMap<>();

    /** path -> response body */
    private final Map<String, String> bodies = new ConcurrentHashMap<>();

    /** path -> per-request delay in millis */
    private final Map<String, Long> slowMillis = new ConcurrentHashMap<>();

    public LocalTestServer() throws IOException {
        this.server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        this.server.createContext("/", new RootHandler());
        this.server.start();
        this.baseUrl = "http://127.0.0.1:" + server.getAddress().getPort();
    }

    public String getBaseUrl() {
        return baseUrl;
    }

    public String url(String path) {
        if (!path.startsWith("/")) {
            path = "/" + path;
        }
        return baseUrl + path;
    }

    public LocalTestServer success(String path, String body) {
        bodies.put(path, body);
        return this;
    }

    /** first {@code failCount} requests of the path fail with 500, then succeed */
    public LocalTestServer transientFail(String path, int failCount, String body) {
        failUntil.put(path, failCount);
        bodies.put(path, body);
        return this;
    }

    /** every request of the path fails with 500 */
    public LocalTestServer alwaysFail(String path) {
        alwaysFail.put(path, Boolean.TRUE);
        return this;
    }

    /** block each request of the path for given millis */
    public LocalTestServer slow(String path, long millis) {
        slowMillis.put(path, millis);
        return this;
    }

    public int hitCount(String path) {
        AtomicInteger counter = hitCounters.get(path);
        return counter == null ? 0 : counter.get();
    }

    @Override
    public void close() {
        server.stop(0);
    }

    private class RootHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            String path = exchange.getRequestURI().getPath();
            int hits = hitCounters.computeIfAbsent(path, k -> new AtomicInteger()).incrementAndGet();

            Long delay = slowMillis.get(path);
            if (delay != null && delay > 0) {
                try {
                    Thread.sleep(delay);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }

            boolean fail = Boolean.TRUE.equals(alwaysFail.get(path))
                    || (failUntil.containsKey(path) && hits <= failUntil.get(path));

            int code = fail ? 500 : 200;
            String body = fail ? "error" : bodies.getOrDefault(path, "ok");
            byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "text/html; charset=utf-8");
            exchange.sendResponseHeaders(code, bytes.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(bytes);
            }
        }
    }
}
