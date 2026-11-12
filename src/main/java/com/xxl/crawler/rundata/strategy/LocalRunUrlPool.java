package com.xxl.crawler.rundata.strategy;

import com.xxl.crawler.rundata.RunUrlPool;
import com.xxl.crawler.rundata.UrlState;
import com.xxl.tool.http.HttpTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;

/**
 * local run url pool
 *
 * <p>URL 原子状态机的本地实现：所有状态流转都在同一把锁内完成，
 * 爬虫线程不直接接触任何内部集合。</p>
 * <ul>
 *     <li>{@code entries}：规范化 URL -> 状态条目，覆盖全部四种状态，承担原子去重；</li>
 *     <li>{@code queue}：仅保存 QUEUED 状态的规范化 URL，FIFO 广度优先；</li>
 *     <li>{@code lock}：队列为空时认领线程在监视器上等待，入队/释放/停止时唤醒。</li>
 * </ul>
 *
 * @author xuxueli 2017-12-14 11:42:23
 */
public class LocalRunUrlPool extends RunUrlPool {
    private static final Logger logger = LoggerFactory.getLogger(LocalRunUrlPool.class);

    /**
     * url state entry
     */
    private static final class UrlEntry {
        /** origin url (un-normalized), keep for request */
        final String url;
        /** lifecycle state */
        UrlState state;
        /** attempted load times (only meaningful while QUEUED/IN_FLIGHT) */
        int attempts;

        UrlEntry(String url, UrlState state) {
            this.url = url;
            this.state = state;
            this.attempts = 0;
        }
    }

    /** normalized url -> entry, all lifecycle states */
    private final Map<String, UrlEntry> entries = new HashMap<>();

    /** normalized urls in QUEUED state, FIFO */
    private final Deque<String> queue = new ArrayDeque<>();

    /** monitor guarding entries/queue/shutdown */
    private final Object lock = new Object();

    /** pool shutdown flag, claimUrl must never block afterwards */
    private volatile boolean shutdown = false;

    // ---------------------- add ----------------------

    /**
     * url add, will not valid
     *
     * @param url           url
     * @param validUrlRegex whether check white url regex
     * @return boolean
     */
    @Override
    public boolean addUrl(String url, boolean validUrlRegex) {
        // valid
        if (!(HttpTool.isHttp(url) || HttpTool.isHttps(url))) {
            logger.debug(">>>>>>>>>>> xxl-crawler isUrl fail, url not valid: {}", url);
            return false;
        }

        if (validUrlRegex) {
            if (!validUrlRegex(url)) {
                logger.debug(">>>>>>>>>>> xxl-crawler validUrlRegex fail, url not valid: {}", url);
                return false;
            }
        }

        String key = normalize(url);
        synchronized (lock) {
            // atomic dedup: any existing state (queued/in-flight/succeeded/failed) rejects re-enqueue
            UrlEntry exist = entries.get(key);
            if (exist != null) {
                logger.debug(">>>>>>>>>>> xxl-crawler addUrl fail, url repeat (state={}): {}", exist.state, url);
                return false;
            }
            entries.put(key, new UrlEntry(url, UrlState.QUEUED));
            queue.addLast(key);
            lock.notifyAll();
        }
        logger.info(">>>>>>>>>>> xxl-crawler addUrl success, url: {}", url);
        return true;
    }

    // ---------------------- claim ----------------------

    /**
     * claim a queued url, atomically QUEUED -> IN_FLIGHT.
     *
     * @return url or null when interrupted / shutdown
     */
    @Override
    public String claimUrl() {
        synchronized (lock) {
            boolean interrupted = false;
            try {
                while (queue.isEmpty() && !shutdown) {
                    try {
                        lock.wait();
                    } catch (InterruptedException e) {
                        interrupted = true;
                        logger.debug(">>>>>>>>>>> xxl-crawler claimUrl interrupted.");
                        return null;
                    }
                }
                if (shutdown) {
                    return null;
                }
                String key = queue.pollFirst();
                if (key == null) {
                    return null;
                }
                UrlEntry entry = entries.get(key);
                if (entry == null || entry.state != UrlState.QUEUED) {
                    // defensive: queue and entries must stay consistent
                    return null;
                }
                entry.state = UrlState.IN_FLIGHT;
                entry.attempts++;
                return entry.url;
            } finally {
                if (interrupted) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    // ---------------------- settle ----------------------

    /**
     * IN_FLIGHT -> SUCCEEDED (terminal)
     *
     * @param url the claimed url
     */
    @Override
    public void succeedUrl(String url) {
        if (url == null) {
            return;
        }
        String key = normalize(url);
        synchronized (lock) {
            UrlEntry entry = entries.get(key);
            if (entry == null || entry.state != UrlState.IN_FLIGHT) {
                logger.warn(">>>>>>>>>>> xxl-crawler succeedUrl ignored, url not in-flight: {}", url);
                return;
            }
            entry.state = UrlState.SUCCEEDED;
            lock.notifyAll();
        }
    }

    /**
     * settle a failed attempt: requeue (IN_FLIGHT -> QUEUED) within retry budget,
     * otherwise mark FAILED (terminal).
     *
     * @param url             the claimed url
     * @param failRetryCount  retry budget, total attempts = failRetryCount + 1
     * @return true if requeued, false if failed finally
     */
    @Override
    public boolean retryUrl(String url, int failRetryCount) {
        if (url == null) {
            return false;
        }
        String key = normalize(url);
        synchronized (lock) {
            UrlEntry entry = entries.get(key);
            if (entry == null || entry.state != UrlState.IN_FLIGHT) {
                logger.warn(">>>>>>>>>>> xxl-crawler retryUrl ignored, url not in-flight: {}", url);
                return false;
            }

            int maxAttempts = failRetryCount + 1;
            if (entry.attempts < maxAttempts) {
                // retry budget remains: atomic requeue
                entry.state = UrlState.QUEUED;
                queue.addLast(key);
                lock.notifyAll();
                logger.info(">>>>>>>>>>> xxl-crawler retryUrl requeue, url: {}, attempts: {}/{}",
                        url, entry.attempts, maxAttempts);
                return true;
            }

            // budget exhausted -> terminal FAILED, url is not lost but never refetched
            entry.state = UrlState.FAILED;
            logger.info(">>>>>>>>>>> xxl-crawler retryUrl give up, url: {}, attempts: {}", url, entry.attempts);
            lock.notifyAll();
            return false;
        }
    }

    /**
     * IN_FLIGHT -> FAILED (terminal)
     *
     * @param url the claimed url
     */
    @Override
    public void failUrl(String url) {
        if (url == null) {
            return;
        }
        String key = normalize(url);
        synchronized (lock) {
            UrlEntry entry = entries.get(key);
            if (entry == null || entry.state != UrlState.IN_FLIGHT) {
                logger.warn(">>>>>>>>>>> xxl-crawler failUrl ignored, url not in-flight: {}", url);
                return;
            }
            entry.state = UrlState.FAILED;
            lock.notifyAll();
        }
    }

    /**
     * IN_FLIGHT -> QUEUED (stop / interrupt), claim is released and never stuck.
     *
     * @param url the claimed url
     */
    @Override
    public void releaseUrl(String url) {
        if (url == null) {
            return;
        }
        String key = normalize(url);
        synchronized (lock) {
            UrlEntry entry = entries.get(key);
            if (entry == null || entry.state != UrlState.IN_FLIGHT) {
                return;
            }
            entry.state = UrlState.QUEUED;
            queue.addLast(key);
            lock.notifyAll();
        }
    }

    /**
     * wake up blocked claimers and forbid further blocking
     */
    @Override
    public void shutdown() {
        synchronized (lock) {
            shutdown = true;
            lock.notifyAll();
        }
    }

    // ---------------------- stats ----------------------

    @Override
    public int getUrlNum() {
        synchronized (lock) {
            return queue.size();
        }
    }

    @Override
    public int getInFlightNum() {
        synchronized (lock) {
            int count = 0;
            for (UrlEntry entry : entries.values()) {
                if (entry.state == UrlState.IN_FLIGHT) {
                    count++;
                }
            }
            return count;
        }
    }

    @Override
    public int getSucceededNum() {
        synchronized (lock) {
            int count = 0;
            for (UrlEntry entry : entries.values()) {
                if (entry.state == UrlState.SUCCEEDED) {
                    count++;
                }
            }
            return count;
        }
    }

    @Override
    public int getFailedNum() {
        synchronized (lock) {
            int count = 0;
            for (UrlEntry entry : entries.values()) {
                if (entry.state == UrlState.FAILED) {
                    count++;
                }
            }
            return count;
        }
    }

}
