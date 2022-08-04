package pe.edu.uni.sources;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.time.LocalDateTime;
import java.util.Random;

public class Ejemplo1DataSources implements SourceFunction<String> {

    Random random = new Random();
    @Override
    public void run(SourceContext<String> sourceContext) throws Exception {
        for (int i=1;i<=1000;i++){
            int id = i;
            LocalDateTime timestamp = LocalDateTime.now();
            String message = String.format("ID: %d Timestamp: %s", i, timestamp.toString());
            sourceContext.collect(message);
            Thread.sleep(random.nextInt(1500));
        }
    }

    @Override
    public void cancel() {

    }
}
