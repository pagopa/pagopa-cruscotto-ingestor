package it.pagopa.cruscotto.ingestion.massivesearch.scheduler;

import it.pagopa.cruscotto.ingestion.massivesearch.config.MassiveSearchProperties;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Component;

import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * Dedicated bounded executor for Massive Search analyses. Kept separate from the Quartz thread pool
 * so the scanner job returns quickly while the (potentially long) executions run here.
 *
 * <p>The parallelism is driven by {@code massive-search.scheduler.max-concurrent-executions}; the
 * queue depth by {@code max-instances-per-run}. When both are saturated, submissions are rejected and
 * the caller keeps the instance in {@code READY} so it is retried on the next scan (no blocking of the
 * Quartz thread).</p>
 */
@Slf4j
@Component
public class MassiveSearchTaskExecutor implements DisposableBean {

    private final ThreadPoolTaskExecutor executor;

    public MassiveSearchTaskExecutor(MassiveSearchProperties properties) {
        MassiveSearchProperties.Scheduler scheduler = properties.getScheduler();
        int concurrency = Math.max(1, scheduler.getMaxConcurrentExecutions());
        int queueCapacity = Math.max(1, scheduler.getMaxInstancesPerRun());

        ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
        taskExecutor.setCorePoolSize(concurrency);
        taskExecutor.setMaxPoolSize(concurrency);
        taskExecutor.setQueueCapacity(queueCapacity);
        taskExecutor.setThreadNamePrefix("massive-search-exec-");
        // Reject (do not block the Quartz scanner thread) when saturated; the instance stays READY.
        taskExecutor.setRejectedExecutionHandler(new ThreadPoolExecutor.AbortPolicy());
        taskExecutor.setWaitForTasksToCompleteOnShutdown(true);
        taskExecutor.setAwaitTerminationSeconds(60);
        taskExecutor.initialize();
        this.executor = taskExecutor;
    }

    /**
     * Submits an execution task.
     *
     * @param task the task to run
     * @return {@code true} when accepted, {@code false} when the executor is saturated (rejected)
     */
    public boolean submit(Runnable task) {
        try {
            executor.execute(task);
            return true;
        } catch (RejectedExecutionException e) {
            return false;
        }
    }

    @Override
    public void destroy() {
        executor.shutdown();
    }
}
