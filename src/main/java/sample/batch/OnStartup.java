package sample.batch;

import org.springframework.batch.core.launch.JobOperator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.ContextRefreshedEvent;

@Configuration
public class OnStartup implements ApplicationListener<ContextRefreshedEvent> {
    
    @Autowired
    JobOperator jobOperator;
    
    @Override
    public void onApplicationEvent(ContextRefreshedEvent event) {
        
        try {
            
            
            
//            System.out.println("ExecId : " + jobOperator.start("job", "time=" + System.currentTimeMillis()));
            
            System.out.println("ExecId : " + jobOperator.restart(2114));
            
            
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        
        
    }
    

}
