package pe.edu.uni.sources;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.json.JSONObject;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class SensorDataSourceFuncion implements SourceFunction<String> {

    int n = 2000;
    Random random = new Random();

    Map<String, List<Integer>> sensorIdsBySensorType = new HashMap<>();
    List<String> sensorTypes = List.of("temp", "co", "pres", "hum");
    Map<String, Double> minValueBySensorType = new HashMap<>();
    Map<String, Double> maxValueBySensorType = new HashMap<>();

    public SensorDataSourceFuncion() {
        sensorIdsBySensorType.put("temp", List.of(1, 2, 3, 4));
        sensorIdsBySensorType.put("co", List.of(5, 6, 7, 8));
        sensorIdsBySensorType.put("pres", List.of(9, 10));
        sensorIdsBySensorType.put("hum", List.of(10, 11, 12));

        minValueBySensorType.put("temp", 20D);
        minValueBySensorType.put("co", 0.5);
        minValueBySensorType.put("pres", 101.0);
        minValueBySensorType.put("hum", 30.0);

        maxValueBySensorType.put("temp", 30D);
        maxValueBySensorType.put("co", 5.0);
        maxValueBySensorType.put("pres", 103.0);
        maxValueBySensorType.put("hum", 50.0);
    }

    private double generateRandomValue(double min, double max){
        //random.nextDouble();
        double scaled = random.nextDouble();
        return (scaled*(max-min)) + min;
    }

    @Override
    public void run(SourceContext<String> sourceContext) throws Exception {
        for (int i = 0; i < n; i++){
            JSONObject object = new JSONObject();
            String randomSensorType = sensorTypes.get(random.nextInt(sensorTypes.size()));
            List<Integer> sensorIds = sensorIdsBySensorType.get(randomSensorType);
            int randomSensorId = sensorIds.get(random.nextInt(sensorIds.size()));
            double maxRange = maxValueBySensorType.get(randomSensorType);
            double minRange = minValueBySensorType.get(randomSensorType);
            double randomValue;
            if (i%25==0 && random.nextBoolean()){
                System.out.println("UNA ANOMALÍA SE HA GENERADO");
                randomValue = maxRange + random.nextInt(20)+10;
            } else {
                randomValue = generateRandomValue(minRange, maxRange);
            }

            LocalDateTime currentTimestamp = LocalDateTime.now();

            object.put("sensorId", randomSensorId);
            object.put("sensorType", randomSensorType);
            object.put("value", randomValue);
            object.put("timestamp", currentTimestamp.atZone(ZoneId.systemDefault()).toEpochSecond());

            sourceContext.collect(object.toString());
            Thread.sleep(random.nextInt(1500));
        }
    }

    @Override
    public void cancel() {

    }

    public static void main(String[] args) {
        SensorDataSourceFuncion sensorDataSourceFuncion = new SensorDataSourceFuncion();
        System.out.println(sensorDataSourceFuncion.generateRandomValue(5,10));
    }
}
