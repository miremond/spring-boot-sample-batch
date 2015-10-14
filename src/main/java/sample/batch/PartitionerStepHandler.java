package sample.batch;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobInstance;
import org.springframework.batch.core.JobInterruptedException;
import org.springframework.batch.core.StartLimitExceededException;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.job.SimpleStepHandler;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.batch.item.ExecutionContext;

public class PartitionerStepHandler extends SimpleStepHandler {
    
    private static final Log logger = LogFactory.getLog(PartitionerStepHandler.class);
    
    private JobRepository jobRepository;

    private ExecutionContext executionContext;

    public PartitionerStepHandler() {
        this(null);
    }

    public PartitionerStepHandler(JobRepository jobRepository) {
        this(jobRepository, new ExecutionContext());
    }

    public PartitionerStepHandler(JobRepository jobRepository, ExecutionContext executionContext) {
        super(jobRepository, executionContext);
        this.jobRepository = jobRepository;
        this.executionContext = executionContext;
    }
    
    private boolean stepExecutionPartOfExistingJobExecution(JobExecution jobExecution, StepExecution stepExecution) {
        return stepExecution != null && stepExecution.getJobExecutionId() != null
                && stepExecution.getJobExecutionId().equals(jobExecution.getId());
    }
    
    @Override
    public StepExecution handleStep(Step step, JobExecution execution)
            throws JobInterruptedException, JobRestartException,
            StartLimitExceededException {
        
        String stepName = step.getName();
        
        if (executionContext != null && executionContext.containsKey(PartitionerConstant.PARTITION_ID_KEY)) {
            stepName += ":" + PartitionerConstant.PARTITION_KEY + executionContext.getInt(PartitionerConstant.PARTITION_ID_KEY);
        }
        
        if (execution.isStopping()) {
            throw new JobInterruptedException("JobExecution interrupted.");
        }

        JobInstance jobInstance = execution.getJobInstance();

        StepExecution lastStepExecution = jobRepository.getLastStepExecution(jobInstance, stepName);
        if (stepExecutionPartOfExistingJobExecution(execution, lastStepExecution)) {
            // If the last execution of this step was in the same job, it's
            // probably intentional so we want to run it again...
            logger.info(String.format("Duplicate step [%s] detected in execution of job=[%s]. "
                    + "If either step fails, both will be executed again on restart.", stepName, jobInstance
                    .getJobName()));
            lastStepExecution = null;
        }
        StepExecution currentStepExecution = lastStepExecution;

        if (shouldStart(lastStepExecution, execution, step)) {

            currentStepExecution = execution.createStepExecution(stepName);

            boolean isRestart = (lastStepExecution != null && !lastStepExecution.getStatus().equals(
                    BatchStatus.COMPLETED));

            if (isRestart) {
                currentStepExecution.setExecutionContext(lastStepExecution.getExecutionContext());

                if(lastStepExecution.getExecutionContext().containsKey("batch.executed")) {
                    currentStepExecution.getExecutionContext().remove("batch.executed");
                }
            }
            else {
                currentStepExecution.setExecutionContext(new ExecutionContext(executionContext));
            }

            jobRepository.add(currentStepExecution);

            logger.info("Executing step: [" + stepName + "]");
            try {
                step.execute(currentStepExecution);
                currentStepExecution.getExecutionContext().put("batch.executed", true);
            }
            catch (JobInterruptedException e) {
                // Ensure that the job gets the message that it is stopping
                // and can pass it on to other steps that are executing
                // concurrently.
                execution.setStatus(BatchStatus.STOPPING);
                throw e;
            }

            jobRepository.updateExecutionContext(execution);

            if (currentStepExecution.getStatus() == BatchStatus.STOPPING
                    || currentStepExecution.getStatus() == BatchStatus.STOPPED) {
                // Ensure that the job gets the message that it is stopping
                execution.setStatus(BatchStatus.STOPPING);
                throw new JobInterruptedException("Job interrupted by step execution");
            }

        }

        return currentStepExecution;
        
    }

}
