package it.pagopa.cruscotto.ingestion.scheduler;

import it.pagopa.cruscotto.ingestion.batch.JobParameterKeys;
import lombok.extern.slf4j.Slf4j;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.quartz.QuartzJobBean;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class QuartzKustoImportJob extends QuartzJobBean {

    @Autowired
    private JobLauncher jobLauncher;

    @Autowired
    private Job kustoImportJob;

    @Override
    protected void executeInternal(JobExecutionContext context) throws JobExecutionException {
        try {
            JobParameters jobParameters = new JobParametersBuilder()
                    .addLong(JobParameterKeys.TIME, System.currentTimeMillis())
                    .toJobParameters();
            jobLauncher.run(kustoImportJob, jobParameters);
            log.info("Kusto import batch job launched successfully via Quartz");
        } catch (Exception e) {
            log.error("Failed to launch Kusto import batch job", e);
            throw new JobExecutionException(e);
        }
    }
}
