package com.xxl.crawler.rundata;

import com.xxl.tool.core.RegexTool;
import com.xxl.tool.http.HttpTool;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * run url pool
 *
 * <p>URL 生命周期原子状态机（queued -> in-flight -> succeeded/failed），状态流转只能由本池完成：
 * 爬虫线程只允许通过 {@link #claimUrl()} 认领 URL，并通过
 * {@link #succeedUrl(String)} / {@link #retryUrl(String, int)} / {@link #failUrl(String)} /
 * {@link #releaseUrl(String)} 结算，不得自行维护任何 URL 集合。</p>
 *
 * <p>自定义实现（如 Redis/DB 共享池）只需实现下列原子方法，即可保留白名单、扩散、
 * 同步/异步等全部上层能力。</p>
 *
 * @author xuxueli 2017-12-14 11:40:50
 */
public abstract class RunUrlPool {

    /**
     * white url regex
     */
    private Set<String> whiteUrlRegexs = Collections.synchronizedSet(new HashSet<String>());

    /**
     * add white url regex
     */
    public void addWhiteUrlRegex(String whiteUrlRegex) {
        whiteUrlRegexs.add(whiteUrlRegex);
    }

    /**
     * valid url, include white url
     *
     * @param url url
     * @return boolean
     */
    public boolean validUrlRegex(String url){
        if (!(HttpTool.isHttp(url) || HttpTool.isHttps(url))) {
            return false;   // false if url invalid
        }

        if (whiteUrlRegexs!=null && !whiteUrlRegexs.isEmpty()) {
            boolean underWhiteUrl = false;
            for (String whiteRegex: whiteUrlRegexs) {
                if (RegexTool.matches(whiteRegex, url)) {
                    underWhiteUrl = true;
                }
            }
            if (!underWhiteUrl) {
                return false;   // check white
            }
        }
        return true;    // true if regex is empty
    }

    // ---------------------- url normalize ----------------------

    /**
     * normalize url, the same normalized url can be claimed only once.
     * 子类可重写以自定义规范化规则（如排序 query 参数、统一末尾斜杠等）。
     *
     * @param url origin url
     * @return normalized url
     */
    protected String normalize(String url) {
        if (url == null) {
            return null;
        }
        String normalized = url.trim();
        int fragmentIdx = normalized.indexOf('#');
        if (fragmentIdx >= 0) {
            normalized = normalized.substring(0, fragmentIdx);
        }
        return normalized;
    }

    // ---------------------- lifecycle state machine ----------------------

    /**
     * add url, with regex valid.
     * 原子去重：相同规范化 URL 只要存在于任意状态（queued/in-flight/succeeded/failed）都拒绝重复入队。
     *
     * @param url           url
     * @param validUrlRegex whether check white url regex
     * @return boolean true if enqueued
     */
    public abstract boolean addUrl(String url, boolean validUrlRegex);

    /**
     * claim a queued url, atomically transfer it from QUEUED to IN_FLIGHT.
     * 池内无 URL 时阻塞等待；停止（{@link #shutdown()}）或线程中断时返回 null。
     *
     * @return claimed url, or null when interrupted / shutdown
     */
    public abstract String claimUrl();

    /**
     * settle an in-flight url as SUCCEEDED (terminal, the normalized url will never be refetched).
     *
     * @param url the claimed url
     */
    public abstract void succeedUrl(String url);

    /**
     * settle a failed attempt of an in-flight url.
     * 池内按认领记录尝试次数：重试额度（failRetryCount）未耗尽时原子重排回 QUEUED，
     * 耗尽时置为 FAILED 终态，URL 不会丢失也不会无限重试。
     *
     * @param url             the claimed url
     * @param failRetryCount  retry budget from RunConf, total attempts = failRetryCount + 1
     * @return true if requeued for another attempt, false if marked FAILED finally
     */
    public abstract boolean retryUrl(String url, int failRetryCount);

    /**
     * settle an in-flight url as FAILED (terminal).
     *
     * @param url the claimed url
     */
    public abstract void failUrl(String url);

    /**
     * release an in-flight claim back to QUEUED (used on stop/interrupt),
     * 保证在途页得到结算、不会永久卡在 in-flight；释放后的 URL 可被后续运行重新认领。
     *
     * @param url the claimed url
     */
    public abstract void releaseUrl(String url);

    /**
     * wake up all threads blocked in {@link #claimUrl()} and forbid new blocking,
     * called when crawler stop. custom pool can override for remote wake-up.
     */
    public void shutdown() {
        // default do nothing
    }

    // ---------------------- stats ----------------------

    /**
     * get queued url num (waiting to be claimed)
     *
     * @return int
     */
    public abstract int getUrlNum();

    /**
     * get in-flight url num (claimed but not settled)
     *
     * @return int
     */
    public abstract int getInFlightNum();

    /**
     * get succeeded url num (terminal). optional stats, default -1 means unsupported.
     *
     * @return int
     */
    public int getSucceededNum() {
        return -1;
    }

    /**
     * get failed url num (terminal). optional stats, default -1 means unsupported.
     *
     * @return int
     */
    public int getFailedNum() {
        return -1;
    }

    /**
     * whether all urls settled: none queued and none in-flight
     *
     * @return boolean
     */
    public boolean isDone() {
        return getUrlNum() == 0 && getInFlightNum() == 0;
    }

    // ---------------------- backward compatibility ----------------------

    /**
     * @deprecated use {@link #claimUrl()} instead; kept only for backward compatibility.
     */
    @Deprecated
    public String getUrl() {
        return claimUrl();
    }

}
