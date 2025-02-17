package org.petrarka.connector.source;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Slf4j
public class CustomDBSourceConnector extends SourceConnector {

    public static final String DB_CONFIG = "dbname";
    public static final String TOPIC_CONFIG = "topic";
    public static final String TASK_BATCH_SIZE_CONFIG = "batchsize";

    public static final int DEFAULT_TASK_BATCH_SIZE = 2000;

    static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(DB_CONFIG, Type.STRING, "jdbc:h2:~/alltransactions", ConfigDef.Importance.HIGH, "Source DB")
            .define(TOPIC_CONFIG, Type.STRING, "kafka-config-topic", new ConfigDef.NonEmptyString(), Importance.HIGH, "The topic to publish data to")
            .define(TASK_BATCH_SIZE_CONFIG, Type.INT, DEFAULT_TASK_BATCH_SIZE, Importance.LOW, "The maximum number of records the source task can read from the file each time it is polled");

    private Map<String, String> props;

    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }

    @Override
    public void start(Map<String, String> props) {
        this.props = props;
        AbstractConfig config = new AbstractConfig(CONFIG_DEF, props);
        String dbName = config.getString(DB_CONFIG);
        log.info("Starting database source connector reading from {}", dbName);
    }

    @Override
    public Class<? extends Task> taskClass() {
        return CustomDBSourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        List<Map<String, String>> configs = new ArrayList<>();
        configs.add(props);
        return configs;
    }

    @Override
    public void stop() {
        // Nothing to do since FileStreamSourceConnector has no background monitoring.
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }
}
