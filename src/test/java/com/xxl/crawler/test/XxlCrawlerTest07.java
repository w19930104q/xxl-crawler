package com.xxl.crawler.test;

import com.xxl.crawler.XxlCrawler;
import com.xxl.crawler.exception.XxlCrawlerException;
import com.xxl.crawler.pageloader.param.Response;
import com.xxl.crawler.pageparser.PageParser;
import com.xxl.crawler.rundata.RunUrlPool;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 爬虫示例07：
 *      - 爬虫名称：集群方式爬取数据
 *      - 爬虫场景：爬取目标页面数据，通过集群方式进行；集群中多个XxlCrawler共享RunUrlPool，协同扩散URL并消费待采集任务，提升采集效率。
 *      - 实现步骤：
 *          1、RunUrlPool 构建： 构建代理池：可人工构造代理池，或通过系统方式更新维护；
 *          2、定义 XxlCrawler：配置开发爬虫代码；
 *          3、PageParser 开发：通过 “afterParse/后处理逻辑” 获取爬虫输出结果数据；
 *
 * (仅供学习测试使用，如有侵犯请联系删除；)
 * @author xuxueli 2017-10-09 19:48:48
 */
public class XxlCrawlerTest07 {
    private static final Logger logger = LoggerFactory.getLogger(XxlCrawlerTest07.class);

    @Test
    public void test01() {

        /**
         * 1、Redis RunUrlPool 构建：通过Redis共享运行数据来实现分布式爬虫，也可以通过其他方式扩展实现，如DB等。（以伪代码进行讲解）
         *
         * 申请以下 Redis 结构，承载 URL 原子状态机（queued/in-flight/succeeded/failed）：
         *      a、queue：List，QUEUED 状态URL，FIFO 待采集
         *      b、state：Hash，规范化URL -> 状态（queued/in-flight/succeeded/failed），承担全局去重
         *      c、attempt：Hash，规范化URL -> 已尝试次数，用于 failRetryCount 重排
         * 所有流转（claim/retry/succeed/fail/release）需通过 Lua 脚本保证原子性。
         */
        RunUrlPool redisRunData = new RunUrlPool() {

            /**
             * 新增一个待采集的URL；state 中已存在任意状态（含 succeeded/failed）都拒绝，全局原子去重。
             */
            @Override
            public boolean addUrl(String url, boolean validUrlRegex){
                /**
                 * TODO（Lua 原子执行）：
                 *      if redis.call('HGET', state, key) then return 0 end
                 *      redis.call('HSET', state, key, 'queued')
                 *      redis.call('RPUSH', queue, key)
                 */
                throw new XxlCrawlerException("not support.");
            }

            /**
             * 认领一个URL，原子地 queued -> in-flight；queue 为空时阻塞等待，shutdown 时返回 null。
             */
            @Override
            public String claimUrl() {
                /**
                 * TODO（BLPOP + Lua 原子流转）：
                 *      local key = redis.call('BLPOP', queue, timeout)
                 *      redis.call('HSET', state, key, 'in-flight')
                 *      redis.call('HINCRBY', attempt, key, 1)
                 *      return originUrl
                 */
                throw new XxlCrawlerException("not support.");
            }

            /**
             * in-flight -> succeeded（终态），相同规范化URL不再抓取。
             */
            @Override
            public void succeedUrl(String url) {
                /**
                 * TODO: redis.call('HSET', state, key, 'succeeded')
                 */
                throw new XxlCrawlerException("not support.");
            }

            /**
             * 失败结算：尝试次数未达 failRetryCount+1 时原子重排回 queue，否则置 failed 终态。
             */
            @Override
            public boolean retryUrl(String url, int failRetryCount) {
                /**
                 * TODO（Lua 原子执行）：
                 *      local attempts = redis.call('HGET', attempt, key)
                 *      if tonumber(attempts) < failRetryCount + 1 then
                 *          redis.call('HSET', state, key, 'queued')
                 *          redis.call('RPUSH', queue, key)
                 *          return 1
                 *      else
                 *          redis.call('HSET', state, key, 'failed')
                 *          return 0
                 *      end
                 */
                throw new XxlCrawlerException("not support.");
            }

            /**
             * in-flight -> failed（终态）。
             */
            @Override
            public void failUrl(String url) {
                /**
                 * TODO: redis.call('HSET', state, key, 'failed')
                 */
                throw new XxlCrawlerException("not support.");
            }

            /**
             * 停止/中断时释放认领：in-flight -> queued，在途页不丢失、不卡死，可被后续运行重新认领。
             */
            @Override
            public void releaseUrl(String url) {
                /**
                 * TODO:
                 *      redis.call('HSET', state, key, 'queued')
                 *      redis.call('RPUSH', queue, key)
                 */
                throw new XxlCrawlerException("not support.");
            }

            /**
             * 获取待采集（QUEUED）URL数量。
             */
            @Override
            public int getUrlNum() {
                /**
                 * TODO: return redis.call('LLEN', queue)
                 */
                throw new XxlCrawlerException("not support.");
            }

            /**
             * 获取在途（IN_FLIGHT）URL数量。
             */
            @Override
            public int getInFlightNum() {
                /**
                 * TODO: 遍历 state 统计 in-flight
                 */
                throw new XxlCrawlerException("not support.");
            }

        };

        /**
         * 2、XxlCrawler 定义
         */
        XxlCrawler crawler = new XxlCrawler.Builder()
                .setRunUrlPool(redisRunData)
                .setUrls("https://gitee.com/explore/all?order=starred&page=1")                              // 设置爬虫入口URL
                .setWhiteUrlRegexs("^https:\\/\\/gitee\\.com\\/explore\\/all\\?order=starred&page=[1-5]$")  // 设置爬虫扩散的URL白名单正则表达式，控制扩散范围
                .setThreadCount(3)
                .setPageParser(new PageParser<Object>() {
                    @Override
                    public void afterParse(Response<Object> response) {
                        logger.info(response.getHtml().baseUri() + "：" + response.getHtml().html());
                    }
                })
                .build();

        crawler.start(true);
    }

}
