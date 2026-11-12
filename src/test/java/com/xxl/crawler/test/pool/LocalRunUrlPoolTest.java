package com.xxl.crawler.test.pool;

import com.xxl.crawler.rundata.RunUrlPool;
import com.xxl.crawler.rundata.strategy.LocalRunUrlPool;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * LocalRunUrlPool atomic state-machine concurrency tests.
 */
public class LocalRunUrlPoolTest {

    private static final int AWAIT_SECONDS = 20;

    /**
     * concurrent duplicate add: the same normalized url is enqueued only once.
     */
    @Test
    public void testConcurrentDuplicateAdd() throws Exception {
        LocalRunUrlPool pool = new LocalRunUrlPool();
        int threads = 32;
        int urlsPerThread = 200;
        int duplicateTimes = 8;
        ExecutorService poolExec = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads);
        AtomicInteger accepted = new AtomicInteger();

        for (int t = 0; t < threads; t++) {
            final int threadIdx = t;
            poolExec.submit(() -> {
                try {
                    start.await();
                    for (int i = 0; i < urlsPerThread; i++) {
                        // every thread races on the same url set, repeated multiple times
                        for (int d = 0; d < duplicateTimes; d++) {
                            String url = "http://example.com/page/" + i;
                            if (pool.addUrl(url, false)) {
                                accepted.incrementAndGet();
                            }
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    done.countDown();
                }
            });
        }
        start.countDown();
        assertTrue(done.await(AWAIT_SECONDS, TimeUnit.SECONDS), "add workers should finish in time");
        poolExec.shutdownNow();

        assertEquals(urlsPerThread, accepted.get(), "only one add per normalized url can win");
        assertEquals(urlsPerThread, pool.getUrlNum(), "all accepted urls stay QUEUED");
        assertEquals(0, pool.getInFlightNum());
    }

    /**
     * high concurrency claim stress: each url is claimed exactly once even with many worker threads.
     */
    @Test
    public void testHighConcurrencyClaimNoDuplicate() throws Exception {
        LocalRunUrlPool pool = new LocalRunUrlPool();
        int urlCount = 500;
        int workers = 64;
        for (int i = 0; i < urlCount; i++) {
            pool.addUrl("http://example.com/item/" + i, false);
        }

        ExecutorService poolExec = Executors.newFixedThreadPool(workers);
        CountDownLatch start = new CountDownLatch(1);
        Set<String> claimed = Collections.newSetFromMap(new ConcurrentHashMap<>());
        List<Future<?>> futures = new ArrayList<>();

        for (int w = 0; w < workers; w++) {
            futures.add(poolExec.submit(() -> {
                try {
                    start.await();
                    while (true) {
                        String url = pool.claimUrl();
                        if (url == null) {
                            return;
                        }
                        assertTrue(claimed.add(url), "duplicate claim detected: " + url);
                        pool.succeedUrl(url);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }));
        }
        start.countDown();

        // stop pool once everything is settled, unblocking idle claimers
        long deadline = System.currentTimeMillis() + AWAIT_SECONDS * 1000L;
        while (System.currentTimeMillis() < deadline && !pool.isDone()) {
            Thread.sleep(20);
        }
        pool.shutdown();
        for (Future<?> f : futures) {
            f.get(AWAIT_SECONDS, TimeUnit.SECONDS);
        }
        poolExec.shutdownNow();

        assertEquals(urlCount, claimed.size(), "every url claimed exactly once");
        assertEquals(urlCount, pool.getSucceededNum());
        assertEquals(0, pool.getFailedNum());
        assertEquals(0, pool.getInFlightNum());
        assertTrue(pool.isDone());
    }

    /**
     * transient failure is requeued per failRetryCount; once settled SUCCEEDED the url is never refetched.
     */
    @Test
    public void testTransientFailureRetryThenSucceed() {
        LocalRunUrlPool pool = new LocalRunUrlPool();
        String url = "http://example.com/retry";
        pool.addUrl(url, false);

        int failRetryCount = 2;   // total 3 attempts
        String first = pool.claimUrl();
        assertEquals(url, first);
        assertEquals(1, pool.getInFlightNum());
        assertTrue(pool.retryUrl(first, failRetryCount), "1st failure should requeue");
        assertEquals(1, pool.getUrlNum());
        assertEquals(0, pool.getInFlightNum());

        String second = pool.claimUrl();
        assertEquals(url, second);
        assertTrue(pool.retryUrl(second, failRetryCount), "2nd failure should requeue");

        String third = pool.claimUrl();
        assertEquals(url, third);
        pool.succeedUrl(third);

        assertEquals(0, pool.getUrlNum());
        assertEquals(0, pool.getInFlightNum());
        assertEquals(1, pool.getSucceededNum());

        // succeeded url can never be re-enqueued
        assertEquals(false, pool.addUrl(url, false), "succeeded url must reject re-add");
        assertEquals(0, pool.getUrlNum());
    }

    /**
     * final failure: after failRetryCount exhausted the url reaches FAILED and is not lost,
     * but it can never be claimed again.
     */
    @Test
    public void testFinalFailureMarksFailed() {
        LocalRunUrlPool pool = new LocalRunUrlPool();
        String url = "http://example.com/fail";
        pool.addUrl(url, false);

        int failRetryCount = 1;   // total 2 attempts
        String c1 = pool.claimUrl();
        assertTrue(pool.retryUrl(c1, failRetryCount));
        String c2 = pool.claimUrl();
        boolean requeued = pool.retryUrl(c2, failRetryCount);
        assertEquals(false, requeued, "exhausted retry budget must settle FAILED");

        assertEquals(0, pool.getUrlNum());
        assertEquals(0, pool.getInFlightNum());
        assertEquals(1, pool.getFailedNum());
        assertEquals(0, pool.getSucceededNum());

        // failed url rejects re-add and is never claimed again
        assertEquals(false, pool.addUrl(url, false));
        // failUrl direct terminal path
        String url2 = "http://example.com/fail2";
        pool.addUrl(url2, false);
        String c3 = pool.claimUrl();
        pool.failUrl(c3);
        assertEquals(2, pool.getFailedNum());
    }

    /**
     * stop/interrupt: blocked claimer returns null and in-flight claim is released back to QUEUED,
     * no entry stays IN_FLIGHT and the run cannot hang.
     */
    @Test
    public void testShutdownAndReleaseNoHang() throws Exception {
        LocalRunUrlPool pool = new LocalRunUrlPool();

        // blocked claimer on empty pool must be woken by shutdown
        ExecutorService poolExec = Executors.newSingleThreadExecutor();
        Future<String> blocked = poolExec.submit(pool::claimUrl);
        Thread.sleep(200L);
        pool.shutdown();
        assertNull(blocked.get(5, TimeUnit.SECONDS), "claimer must return null after shutdown");

        // release an in-flight claim back to QUEUED
        LocalRunUrlPool pool2 = new LocalRunUrlPool();
        String url = "http://example.com/inflight";
        pool2.addUrl(url, false);
        String claimed = pool2.claimUrl();
        assertEquals(url, claimed);
        assertEquals(1, pool2.getInFlightNum());
        pool2.releaseUrl(claimed);
        assertEquals(0, pool2.getInFlightNum());
        assertEquals(1, pool2.getUrlNum());

        // re-claim after release works
        String reclaimed = pool2.claimUrl();
        assertEquals(url, reclaimed);
        pool2.succeedUrl(reclaimed);
        assertEquals(1, pool2.getSucceededNum());

        poolExec.shutdownNow();
    }

    /**
     * normalized urls differing only by fragment are treated as one claim.
     */
    @Test
    public void testNormalizedDedup() {
        RunUrlPool pool = new LocalRunUrlPool();
        assertTrue(pool.addUrl("http://example.com/a?x=1", false));
        assertEquals(false, pool.addUrl("http://example.com/a?x=1#section", false),
                "fragment difference must not create a second claim");
        assertEquals(1, pool.getUrlNum());
    }

    /**
     * concurrent mixed lifecycle: racing duplicate adds against many claiming workers
     * prove no duplicate and no loss; a single-worker phase proves the retry round-trip.
     */
    @Test
    public void testConcurrentMixedLifecycle() throws Exception {
        LocalRunUrlPool pool = new LocalRunUrlPool();
        int urlCount = 300;
        int workers = 24;
        Set<String> urls = new HashSet<>();
        for (int i = 0; i < urlCount; i++) {
            String url = "http://example.com/mix/" + i;
            urls.add(url);
            pool.addUrl(url, false);
        }
        // racing duplicate adds while claims are in progress
        ExecutorService addExec = Executors.newFixedThreadPool(8);
        CountDownLatch addStart = new CountDownLatch(1);
        for (int t = 0; t < 8; t++) {
            addExec.submit(() -> {
                try {
                    addStart.await();
                    for (String url : urls) {
                        pool.addUrl(url, false);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }
        addStart.countDown();

        ExecutorService workExec = Executors.newFixedThreadPool(workers);
        CountDownLatch workStart = new CountDownLatch(1);
        Set<String> claimed = Collections.newSetFromMap(new ConcurrentHashMap<>());
        List<Future<?>> futures = new ArrayList<>();
        for (int w = 0; w < workers; w++) {
            futures.add(workExec.submit(() -> {
                try {
                    workStart.await();
                    while (true) {
                        String url = pool.claimUrl();
                        if (url == null) {
                            return;
                        }
                        assertTrue(claimed.add(url), "duplicate claim under concurrency: " + url);
                        pool.succeedUrl(url);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }));
        }
        workStart.countDown();

        long deadline = System.currentTimeMillis() + AWAIT_SECONDS * 1000L;
        while (System.currentTimeMillis() < deadline && !pool.isDone()) {
            Thread.sleep(20);
        }
        pool.shutdown();
        for (Future<?> f : futures) {
            f.get(AWAIT_SECONDS, TimeUnit.SECONDS);
        }
        addExec.shutdownNow();
        workExec.shutdownNow();

        assertEquals(urlCount, claimed.size(), "no duplicate, no loss under concurrent add+claim");
        assertEquals(urlCount, pool.getSucceededNum());
        assertEquals(0, pool.getInFlightNum());
        assertEquals(0, pool.getUrlNum());

        // deterministic retry round-trip on a fresh pool with one worker:
        // failure -> requeue -> re-claim -> success, attempts bounded by failRetryCount
        LocalRunUrlPool retryPool = new LocalRunUrlPool();
        retryPool.addUrl("http://example.com/retry-mix", false);
        String first = retryPool.claimUrl();
        assertTrue(retryPool.retryUrl(first, 1), "first failure requeues within budget");
        assertEquals(1, retryPool.getUrlNum());
        String second = retryPool.claimUrl();
        assertEquals(first, second, "single worker re-claims the requeued url");
        retryPool.succeedUrl(second);
        assertEquals(1, retryPool.getSucceededNum());
        assertEquals(0, retryPool.getFailedNum());
        assertEquals(0, retryPool.getInFlightNum());
    }
}
