package com.xxl.crawler.rundata;

/**
 * url lifecycle state
 *
 * <pre>
 *
 *      addUrl                 claimUrl                 retryUrl (retry budget remains)
 *   (none) -----> QUEUED ----------------> IN_FLIGHT ------------------------------> QUEUED
 *                    ^                       |   |
 *                    |                       |   | succeedUrl
 *                    | releaseUrl (stop)     |   v
 *                    +-----------------------+  SUCCEEDED   (terminal, never refetch)
 *                                            |
 *                                            | retryUrl (retry budget exhausted) / failUrl
 *                                            v
 *                                          FAILED      (terminal)
 *
 * </pre>
 *
 * @author xuxueli 2026-11-12
 */
public enum UrlState {

    /** 已入队、等待被认领 */
    QUEUED,

    /** 已被某个爬虫线程认领、正在加载/解析（在途） */
    IN_FLIGHT,

    /** 加载并解析成功，生命周期终结，相同规范化URL不再抓取 */
    SUCCEEDED,

    /** 重试额度耗尽后的失败终态 */
    FAILED

}
