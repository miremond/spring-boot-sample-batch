package sample.batch;

import org.springframework.batch.core.JobExecutionException;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.job.StepHandler;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.job.flow.FlowExecutionException;
import org.springframework.batch.core.job.flow.FlowExecutor;
import org.springframework.batch.core.job.flow.FlowStep;
import org.springframework.batch.core.job.flow.JobFlowExecutor;

public class PartitionerFlowStep extends FlowStep {
    
    private Flow flow;

    @Override
    public void setFlow(Flow flow) {
        super.setFlow(flow);
        this.flow = flow;
    }
    
    @Override
    protected void doExecute(StepExecution stepExecution) throws Exception {
        
        try {
            stepExecution.getExecutionContext().put(STEP_TYPE_KEY, this.getClass().getName());
            StepHandler stepHandler = new PartitionerStepHandler(getJobRepository(), stepExecution.getExecutionContext());
            FlowExecutor executor = new JobFlowExecutor(getJobRepository(), stepHandler, stepExecution.getJobExecution());
            executor.updateJobExecutionStatus(flow.start(executor).getStatus());
            stepExecution.upgradeStatus(executor.getJobExecution().getStatus());
            stepExecution.setExitStatus(executor.getJobExecution().getExitStatus());
        }
        catch (FlowExecutionException e) {
            if (e.getCause() instanceof JobExecutionException) {
                throw (JobExecutionException) e.getCause();
            }
            throw new JobExecutionException("Flow execution ended unexpectedly", e);
        }
        
    }

}
