package experiment.component;

import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * 获取系统当前时间，以毫秒为单位，每毫秒更新，性能高一些
 *     用单个调度线程来按毫秒更新时间戳，相当于维护一个全局缓存。
 *     其他线程取时间戳时相当于从内存取，不会再造成时钟资源的争用，代价就是牺牲了一些精确度。
 *     使用的时候，直接 CurrentTimeMillisClock.getInstance().now() 就可以了。
 *     不过，在System.currentTimeMillis()的效率没有影响程序整体的效率时，就不必忙着做优化，这只是为极端情况准备的。
 *     详情见：https://www.cnblogs.com/javastack/p/14131457.html
 */
public class CurrentTimeMillisClock {
    private volatile long now;

    private CurrentTimeMillisClock() {
        this.now = System.currentTimeMillis();
        scheduleTick();
    }

    private void scheduleTick() {
        new ScheduledThreadPoolExecutor(1, runnable -> {
            Thread thread = new Thread(runnable, "current-time-millis");
            thread.setDaemon(true);
            return thread;
        }).scheduleAtFixedRate(() -> {
            now = System.currentTimeMillis();
        }, 1, 1, TimeUnit.MILLISECONDS);
    }

    public long now() {
        return now;
    }

    public static CurrentTimeMillisClock getInstance() {
        return SingletonHolder.INSTANCE;
    }

    private static class SingletonHolder {
        private static final CurrentTimeMillisClock INSTANCE = new CurrentTimeMillisClock();
    }
}
