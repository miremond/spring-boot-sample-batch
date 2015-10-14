package sample.batch;

import java.util.HashMap;
import java.util.Map;

import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.item.ExecutionContext;


public class SimplePartitioner implements Partitioner {

	

	@Override
	public Map<String, ExecutionContext> partition(int gridSize) {
		Map<String, ExecutionContext> map = new HashMap<String, ExecutionContext>(gridSize);
		for (int i = 0; i < gridSize; i++) {
			ExecutionContext ec = new ExecutionContext();
			ec.putInt(PartitionerConstant.PARTITION_ID_KEY, i);
            map.put(PartitionerConstant.PARTITION_KEY + i, ec);
		}
		return map;
	}

}
