package com.xxl.crawler.test.pool;

import com.xxl.crawler.XxlCrawler;
import com.xxl.crawler.pageloader.param.Response;
import com.xxl.crawler.pageloader.strategy.JsoupPageLoader;
import com.xxl.crawler.pageparser.PageParser;
import com.xxl.crawler.rundata.strategy.LocalRunUrlPool;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * end-to-end regression over a local http server, verifying the atomic URL state machine
 * through the real PageLoader/PageParser and sync/async entries of XxlCrawler.
 */
public class XxlCrawlerPoolRegressionTest {

    private static final int AWAIT_SECONDS = 30;

    /** parser that records every successfully parsed url exactly once */
    private static class RecordingParser extends PageParser<Object> {
        final Set<String> parsed = Collections.newSetFromMap(new ConcurrentHashMap<>());
        final AtomicInteger afterFail = new AtomicInteger();

        @Override
        public void afterParse(Response<Object> response) {
            parsed.add(response.getRequest().getUrl());
        }

        @Override
        public void afterParseFail(Response<Object> response) {
            afterFail.incrementAndGet();
        }
    }

    private static String pageWithLinks(LocalTestServer server, String... paths) {
        StringBuilder sb = new StringBuilder("<html><body>");
        for (String p : paths) {
            sb.append("<a href=\"").append(server.url(p)).append("\">link</a>");
        }
        sb.append("</body></html>");
        return sb.toString();
    }

    private static void awaitSettled(LocalRunUrlPool pool, long timeoutMillis) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline && !pool.isDone()) {
            Thread.sleep(20);
        }
        assertTrue(pool.isDone(), "pool should settle (no queued/in-flight url) within timeout");
    }

    /**
     * concurrent duplicate links: a page repeatedly links the same target urls;
     * each normalized target is fetched exactly once.
     */
    @Test
    public void testConcurrentDuplicateLinks() throws Exception {
        try (LocalTestServer server = new LocalTestServer()) {
            int targetCount = 20;
            // index page lists every target 10 times, duplicated across the body
            StringBuilder indexBody = new StringBuilder("<html><body>");
            for (int round = 0; round < 10; round++) {
                for (int i = 0; i < targetCount; i++) {
                    indexBody.append("<a href=\"").append(server.url("/target/" + i)).append("\">x</a>");
                }
            }
            indexBody.append("</body></html>");
            server.success("/index", indexBody.toString());
            for (int i = 0; i < targetCount; i++) {
                server.success("/target/" + i, pageWithLinks(server, "/index"));
            }

            LocalRunUrlPool pool = new LocalRunUrlPool();
            RecordingParser parser = new RecordingParser();
            XxlCrawler crawler = new XxlCrawler.Builder()
                    .setRunUrlPool(pool)
                    .setUrls(server.url("/index"))
                    .setAllowSpread(true)
                    .setThreadCount(8)
                    .setPauseMillis(0)
                    .setPageLoader(new JsoupPageLoader())
                    .setPageParser(parser)
                    .build();
            crawler.start(true);

            awaitSettled(pool, AWAIT_SECONDS * 1000L);
            assertEquals(targetCount + 1, parser.parsed.size(), "index + each distinct target parsed once");
            for (int i = 0; i < targetCount; i++) {
                assertEquals(1, server.hitCount("/target/" + i), "duplicate links must not cause repeat fetch: /target/" + i);
            }
            assertEquals(0, pool.getInFlightNum());
            assertEquals(targetCount + 1, pool.getSucceededNum());
            assertEquals(0, pool.getFailedNum());
        }
    }

    /**
     * transient failure: page fails the first 2 attempts then succeeds with failRetryCount=2;
     * no url is lost and the final successful page is not refetched.
     */
    @Test
    public void testTransientFailureRecovered() throws Exception {
        try (LocalTestServer server = new LocalTestServer()) {
            server.transientFail("/flaky", 2, "<html><body>ok</body></html>");

            LocalRunUrlPool pool = new LocalRunUrlPool();
            RecordingParser parser = new RecordingParser();
            XxlCrawler crawler = new XxlCrawler.Builder()
                    .setRunUrlPool(pool)
                    .setUrls(server.url("/flaky"))
                    .setAllowSpread(false)
                    .setThreadCount(2)
                    .setPauseMillis(0)
                    .setFailRetryCount(2)          // total 3 attempts
                    .setPageLoader(new JsoupPageLoader())
                    .setPageParser(parser)
                    .build();
            crawler.start(true);

            awaitSettled(pool, AWAIT_SECONDS * 1000L);
            assertEquals(3, server.hitCount("/flaky"), "two transient failures then one success");
            assertEquals(1, parser.parsed.size(), "recovered page parsed once after retries");
            assertEquals(1, pool.getSucceededNum());
            assertEquals(0, pool.getFailedNum());
            assertEquals(2, parser.afterFail.get(), "afterParseFail invoked for the two failed attempts");
        }
    }

    /**
     * final failure: page always fails; after failRetryCount it is marked FAILED,
     * counted, not retried forever and never blocks shutdown.
     */
    @Test
    public void testFinalFailureTerminates() throws Exception {
        try (LocalTestServer server = new LocalTestServer()) {
            server.alwaysFail("/dead");

            LocalRunUrlPool pool = new LocalRunUrlPool();
            RecordingParser parser = new RecordingParser();
            XxlCrawler crawler = new XxlCrawler.Builder()
                    .setRunUrlPool(pool)
                    .setUrls(server.url("/dead"))
                    .setAllowSpread(false)
                    .setThreadCount(2)
                    .setPauseMillis(0)
                    .setFailRetryCount(2)          // total 3 attempts
                    .setPageLoader(new JsoupPageLoader())
                    .setPageParser(parser)
                    .build();
            crawler.start(true);

            awaitSettled(pool, AWAIT_SECONDS * 1000L);
            assertEquals(3, server.hitCount("/dead"), "must stop retrying after budget exhausted");
            assertEquals(0, parser.parsed.size(), "failed page never parsed successfully");
            assertEquals(1, pool.getFailedNum());
            assertEquals(0, pool.getSucceededNum());
            assertEquals(0, pool.getInFlightNum());
        }
    }

    /**
     * stop interleaving: async run is stopped while pages are in-flight;
     * the run returns without hanging, in-flight claims are released, no entry stays IN_FLIGHT.
     */
    @Test
    public void testStopInterleavingReleasesInFlight() throws Exception {
        try (LocalTestServer server = new LocalTestServer()) {
            // slow pages keep urls in-flight when stop arrives
            for (int i = 0; i < 6; i++) {
                server.slow("/slow/" + i, 1500L).success("/slow/" + i, "<html><body>slow</body></html>");
            }

            LocalRunUrlPool pool = new LocalRunUrlPool();
            RecordingParser parser = new RecordingParser();
            XxlCrawler crawler = new XxlCrawler.Builder()
                    .setRunUrlPool(pool)
                    .setUrls(server.url("/slow/0"), server.url("/slow/1"), server.url("/slow/2"),
                            server.url("/slow/3"), server.url("/slow/4"), server.url("/slow/5"))
                    .setAllowSpread(false)
                    .setThreadCount(4)
                    .setPauseMillis(0)
                    .setPageLoader(new JsoupPageLoader())
                    .setPageParser(parser)
                    .build();

            // async entry
            crawler.start(false);
            // stop while some pages are in-flight and others still queued
            Thread.sleep(300L);
            long stopAt = System.currentTimeMillis();
            crawler.stop();

            // no thread may remain blocked in claimUrl
            long deadline = System.currentTimeMillis() + AWAIT_SECONDS * 1000L;
            while (System.currentTimeMillis() < deadline && pool.getInFlightNum() > 0) {
                Thread.sleep(20);
            }
            long elapsed = System.currentTimeMillis() - stopAt;
            assertTrue(elapsed < AWAIT_SECONDS * 1000L, "stop must not hang");
            assertEquals(0, pool.getInFlightNum(), "in-flight claims must be released on stop, none stuck");
            // released urls return to QUEUED (re-runnable), none lost
            assertTrue(pool.getUrlNum() >= 1, "unfinished urls should be released back to QUEUED, got queued="
                    + pool.getUrlNum());
        }
    }

    /**
     * whitelist + spread: only links matching the white regex are enqueued/parsed,
     * non-whitelist spread links are filtered out; each allowed url is fetched once.
     */
    @Test
    public void testWhitelistAndSpread() throws Exception {
        try (LocalTestServer server = new LocalTestServer()) {
            // root -> allowed page (in white) + blocked page (out of white, must be filtered)
            server.success("/root", pageWithLinks(server, "/allow/a", "/block/b"));
            server.success("/allow/a", pageWithLinks(server, "/allow/c"));
            server.success("/block/b", pageWithLinks(server, "/allow/c"));
            server.success("/allow/c", "<html><body>leaf</body></html>");

            LocalRunUrlPool pool = new LocalRunUrlPool();
            RecordingParser parser = new RecordingParser();
            XxlCrawler crawler = new XxlCrawler.Builder()
                    .setRunUrlPool(pool)
                    .setUrls(server.url("/root"))
                    .setWhiteUrlRegexs(server.getBaseUrl() + "/(root|allow/.*)")
                    .setAllowSpread(true)
                    .setThreadCount(3)
                    .setPauseMillis(0)
                    .setPageLoader(new JsoupPageLoader())
                    .setPageParser(parser)
                    .build();
            crawler.start(true);

            awaitSettled(pool, AWAIT_SECONDS * 1000L);

            // non-whitelist spread link is filtered at enqueue and never fetched
            assertEquals(0, server.hitCount("/block/b"), "non-whitelist url must not be enqueued nor fetched");
            // whitelist pages all reached and parsed exactly once
            assertTrue(parser.parsed.contains(server.url("/root")));
            assertTrue(parser.parsed.contains(server.url("/allow/a")));
            assertTrue(parser.parsed.contains(server.url("/allow/c")));
            assertEquals(1, server.hitCount("/root"));
            assertEquals(1, server.hitCount("/allow/a"));
            assertEquals(1, server.hitCount("/allow/c"));
            // even though both /root and /allow/a link to /allow/c, it is fetched only once
            assertEquals(3, pool.getSucceededNum());
            assertEquals(0, pool.getFailedNum());
            assertEquals(0, pool.getInFlightNum());
        }
    }

    /**
     * high concurrency duplicate stress: many workers race on a large duplicated link graph;
     * every distinct url is fetched exactly once and all settle to a terminal state.
     */
    @Test
    public void testHighConcurrencyDuplicateStress() throws Exception {
        try (LocalTestServer server = new LocalTestServer()) {
            int targetCount = 100;
            StringBuilder indexBody = new StringBuilder("<html><body>");
            // heavy duplication: each target linked 20 times, plus inter-linking targets
            for (int round = 0; round < 20; round++) {
                for (int i = 0; i < targetCount; i++) {
                    indexBody.append("<a href=\"").append(server.url("/p/" + i)).append("\">x</a>");
                }
            }
            indexBody.append("</body></html>");
            server.success("/index", indexBody.toString());

            // each target links back to index and a few neighbors, all duplicated
            for (int i = 0; i < targetCount; i++) {
                StringBuilder body = new StringBuilder("<html><body>");
                for (int round = 0; round < 5; round++) {
                    body.append("<a href=\"").append(server.url("/index")).append("\">home</a>");
                    int neighbor = (i + 1) % targetCount;
                    body.append("<a href=\"").append(server.url("/p/" + neighbor)).append("\">n</a>");
                }
                body.append("</body></html>");
                server.success("/p/" + i, body.toString());
            }

            LocalRunUrlPool pool = new LocalRunUrlPool();
            RecordingParser parser = new RecordingParser();
            XxlCrawler crawler = new XxlCrawler.Builder()
                    .setRunUrlPool(pool)
                    .setUrls(server.url("/index"))
                    .setAllowSpread(true)
                    .setThreadCount(32)
                    .setPauseMillis(0)
                    .setPageLoader(new JsoupPageLoader())
                    .setPageParser(parser)
                    .build();

            ExecutorService runner = Executors.newSingleThreadExecutor();
            CountDownLatch started = new CountDownLatch(1);
            runner.submit(() -> {
                started.countDown();
                crawler.start(true);
            });
            started.await();

            awaitSettled(pool, 60_000L);
            runner.shutdown();
            runner.awaitTermination(AWAIT_SECONDS, TimeUnit.SECONDS);

            assertEquals(targetCount + 1, parser.parsed.size(),
                    "index + all distinct targets parsed exactly once under high concurrency");
            assertEquals(1, server.hitCount("/index"), "index fetched once");
            for (int i = 0; i < targetCount; i++) {
                assertEquals(1, server.hitCount("/p/" + i), "target fetched once: /p/" + i);
            }
            assertEquals(targetCount + 1, pool.getSucceededNum());
            assertEquals(0, pool.getFailedNum());
            assertEquals(0, pool.getInFlightNum());
        }
    }
}
