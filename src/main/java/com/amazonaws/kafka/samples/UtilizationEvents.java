package com.amazonaws.kafka.samples;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import samples.workspaceevent.avro.WorkspaceEvent;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class UtilizationEvents {

    static final int MODEL_COUNT_PER_WKSP = 5;
    static final private String[] state = {"open"};
    private Random rand = new Random();
    private final static AtomicLong totalEvents = new AtomicLong(0L);
    private AtomicInteger errorCount = new AtomicInteger(0);
    private AtomicInteger eventCount = new AtomicInteger(0);
    private final Map<TopicPartition, Long> lastOffset = new HashMap<>();

    private Long previousGlobalSeqNo = 0L;
    private static final Logger logger = LogManager.getLogger(UtilizationEvents.class);
    int getThreadEventCount(){
        return eventCount.intValue();
    }

    int getErrorCount(){
        return errorCount.intValue();
    }

    Map<TopicPartition, Long> getLastOffset(){
        return lastOffset;
    }

    Long getLastGlobalSeqNo(){
        return previousGlobalSeqNo;
    }

    void genEvents(Producer<String, WorkspaceEvent> kafkaProducer, Integer workspaceId) throws Exception {

        WorkspaceEvent event;
        String stateId = state[rand.nextInt(state.length)];
        String modelId = String.valueOf(100 + rand.nextInt(MODEL_COUNT_PER_WKSP));
        long eventTimestamp = System.currentTimeMillis()/1000;
        String version = String.valueOf(eventTimestamp);
        double memoryUtil = 60.0 + rand.nextInt(30);
        double cpuUtil = 55.0 + rand.nextInt(30);
        do {

            event = genWorkspaceEvent(String.valueOf(workspaceId), modelId, stateId, version, memoryUtil, cpuUtil, eventTimestamp);
            previousGlobalSeqNo = KafkaEventstreamClient.counter.incrementAndGet();
            String localEventFileLocation = "/tmp/WorkspaceEvent.txt";
            if (!KafkaEventstreamClient.nologgingEvents)
                Util.eventWriter(event.toString(), localEventFileLocation, true, "workspaceEvent");

            kafkaProducer.send(new ProducerRecord<>(KafkaEventstreamClient.topic, workspaceId.toString(), event), (recordMetadata, e) -> {
                if (e != null) {
                    logger.error(Util.stackTrace(e));
                    errorCount.getAndIncrement();
                } else {
                    if (recordMetadata.hasOffset()) {
                        lastOffset.put(new TopicPartition(recordMetadata.topic(), recordMetadata.partition()), recordMetadata.offset());
                        eventCount.getAndIncrement();
                        totalEvents.getAndIncrement();
                    }
                }
            });

        } while (!(event.getModelId().toString().equals("")) && errorCount.get() < 1);

    }

    private WorkspaceEvent genWorkspaceEvent(String workspaceId, String modelId, String stateId, String version, double memoryUtil, double cpuUtil, long eventTimestamp) {
        return WorkspaceEvent.newBuilder().setWorkspaceId(workspaceId).setModelId(modelId).setState(stateId).setVersion(version).setMemUtil(memoryUtil).setCpuUtl(cpuUtil).setEventTimestamp(eventTimestamp).build();
    }

}
