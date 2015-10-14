/*
 * Copyright 2012-2015 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package sample.batch;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.configuration.JobRegistry;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.support.JobRegistryBeanPostProcessor;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
@EnableBatchProcessing
public class SampleBatchApplication {

	@Autowired
	private JobBuilderFactory jobs;

	@Autowired
	private StepBuilderFactory steps;
	@Autowired
    private JobRepository jobRepository;
	
	@Bean
	protected JobRegistryBeanPostProcessor postProcessor(JobRegistry jobRegistry, BeanFactory beanFactory) {
	    JobRegistryBeanPostProcessor processor = new JobRegistryBeanPostProcessor();
	    processor.setJobRegistry(jobRegistry);
	    processor.setBeanFactory(beanFactory);
	    return processor;
	}

	@Bean
	protected Tasklet tasklet() {
		return new Tasklet() {
			@Override
			public RepeatStatus execute(StepContribution contribution,
					ChunkContext context) {
				return RepeatStatus.FINISHED;
			}
		};
	}
	
	@Bean
    protected Tasklet subtasklet1() {
        return new Tasklet() {
            @Override
            public RepeatStatus execute(StepContribution contribution,
                    ChunkContext context) {
                
                ExecutionContext stepEc = context.getStepContext().getStepExecution().getExecutionContext();
                if (stepEc.getInt(PartitionerConstant.PARTITION_ID_KEY) == 1) throw new RuntimeException(); 
                
                return RepeatStatus.FINISHED;
            }
        };
    }

	@Bean
	public Job job() throws Exception {
		return this.jobs.get("job")
		        .start(partition())
		        .build();
	}

	@Bean
    protected Step partition() throws Exception {
        return this.steps.get("partition")
                .partitioner("partition", partitioner())
                .gridSize(2)
                .step(flowStep())
                .build();
    }
    
    @Bean
    protected Partitioner partitioner() {
        return new SimplePartitioner();
    }
    
    @Bean
    protected Step flowStep() throws Exception {
        
        PartitionerFlowStep step = new PartitionerFlowStep();
        step.setFlow(flow());
        step.setJobRepository(jobRepository);
        
        return step;
    }
	
	@Bean
    public Flow flow() throws Exception {
        return new FlowBuilder<Flow>("flow")
                .start(subStep1())
                .next(subStep2())
                .build();
    }
	
	@Bean
    protected Step subStep1() throws Exception {
        return this.steps.get("subStep1").tasklet(subtasklet1()).build();
    }
	
	@Bean
    protected Step subStep2() throws Exception {
        return this.steps.get("subStep2").tasklet(tasklet()).build();
    }

	public static void main(String[] args) throws Exception {
		SpringApplication.run(SampleBatchApplication.class, args);
	}

}
