package com.coursework;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.time.Duration;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public final class TrafficWindowJob {
    private TrafficWindowJob() {
    }

    public static void main(String[] args) throws Exception {
        ParameterTool parameters = ParameterTool.fromArgs(args);
        String bootstrapServers = parameters.get("bootstrap-servers", "kafka:9092");
        String topic = parameters.get("topic", "traffic-telemetry");

        StreamExecutionEnvironment environment =
                StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(3);
        environment.enableCheckpointing(60_000L);
        environment.getConfig().setAutoWatermarkInterval(1_000L);
        environment.getConfig().setGlobalJobParameters(parameters);

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(topic)
                .setGroupId("traffic-window-totals")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<TrafficEvent> telemetry = environment
                .fromSource(
                        source,
                        WatermarkStrategy.noWatermarks(),
                        "traffic-telemetry-source")
                .map(new ParseTelemetry())
                .returns(TypeInformation.of(TrafficEvent.class))
                .name("parse-structured-traffic-json");

        WatermarkStrategy<TrafficEvent> watermarkStrategy =
                WatermarkStrategy.<TrafficEvent>forBoundedOutOfOrderness(
                                Duration.ofSeconds(10))
                        .withTimestampAssigner(
                                (event, previousTimestamp) -> event.getEventTimestampMs())
                        .withIdleness(Duration.ofSeconds(30));

        telemetry
                .assignTimestampsAndWatermarks(watermarkStrategy)
                .name("ten-second-bounded-out-of-orderness")
                .keyBy(TrafficEvent::getSensorId)
                .window(TumblingEventTimeWindows.of(Time.minutes(10)))
                .aggregate(new VehicleCountSum(), new FormatWindowTotal())
                .name("ten-minute-moving-total-by-sensor")
                .print()
                .name("print-ten-minute-sensor-totals");

        environment.execute("Austin traffic telemetry: 10-minute sensor totals");
    }

    public static class TrafficEvent {
        private String sensorId;
        private long vehicleCount;
        private long eventTimestampMs;

        public TrafficEvent() {
        }

        public TrafficEvent(String sensorId, long vehicleCount, long eventTimestampMs) {
            this.sensorId = sensorId;
            this.vehicleCount = vehicleCount;
            this.eventTimestampMs = eventTimestampMs;
        }

        public String getSensorId() {
            return sensorId;
        }

        public void setSensorId(String sensorId) {
            this.sensorId = sensorId;
        }

        public long getVehicleCount() {
            return vehicleCount;
        }

        public void setVehicleCount(long vehicleCount) {
            this.vehicleCount = vehicleCount;
        }

        public long getEventTimestampMs() {
            return eventTimestampMs;
        }

        public void setEventTimestampMs(long eventTimestampMs) {
            this.eventTimestampMs = eventTimestampMs;
        }
    }

    private static final class ParseTelemetry implements MapFunction<String, TrafficEvent> {
        private transient ObjectMapper mapper;

        @Override
        public TrafficEvent map(String message) throws Exception {
            JsonNode event = mapper().readTree(message);
            JsonNode sensorNode = required(event, "sensor_id");
            JsonNode countNode = required(event, "vehicle_count");
            JsonNode timestampNode = required(event, "event_timestamp_ms");

            String sensorId = sensorNode.asText().trim();
            if (sensorId.isEmpty()) {
                throw new IllegalArgumentException("sensor_id must not be empty");
            }
            if (!countNode.isIntegralNumber() || !timestampNode.isIntegralNumber()) {
                throw new IllegalArgumentException(
                        "vehicle_count and event_timestamp_ms must be integral JSON numbers");
            }

            return new TrafficEvent(
                    sensorId,
                    countNode.longValue(),
                    timestampNode.longValue());
        }

        private ObjectMapper mapper() {
            if (mapper == null) {
                mapper = new ObjectMapper();
            }
            return mapper;
        }

        private static JsonNode required(JsonNode event, String field) {
            JsonNode value = event.get(field);
            if (value == null || value.isNull()) {
                throw new IllegalArgumentException("missing required JSON field: " + field);
            }
            return value;
        }
    }

    private static final class VehicleCountSum
            implements AggregateFunction<TrafficEvent, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(TrafficEvent event, Long accumulator) {
            return accumulator + event.getVehicleCount();
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long first, Long second) {
            return first + second;
        }
    }

    private static final class FormatWindowTotal
            extends ProcessWindowFunction<Long, String, String, TimeWindow> {
        private transient ObjectMapper mapper;

        @Override
        public void process(
                String sensorId,
                Context context,
                Iterable<Long> totals,
                Collector<String> output) throws Exception {
            long total = totals.iterator().next();
            ObjectNode result = mapper().createObjectNode();
            result.put("sensor_id", sensorId);
            result.put("window_start_ms", context.window().getStart());
            result.put("window_end_ms", context.window().getEnd());
            result.put("vehicle_count_total", total);
            output.collect(mapper().writeValueAsString(result));
        }

        private ObjectMapper mapper() {
            if (mapper == null) {
                mapper = new ObjectMapper();
            }
            return mapper;
        }
    }
}
