package pe.edu.uni;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import pe.edu.uni.sources.Ejemplo1DataSources;
import pe.edu.uni.sources.SensorDataSourceFuncion;

public class Ejemplo1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //DataStream<String> inputDataStream = env.addSource(new Ejemplo1DataSources());
        DataStream<String> inputDataStream = env.addSource(new SensorDataSourceFuncion());

        inputDataStream.print();

        env.execute(Ejemplo1.class.getName());
    }
}
