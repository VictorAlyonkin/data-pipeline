package org.petrarka.connector.source;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.petrarka.config.Config;

import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@Slf4j
public class CustomDBSourceTask extends SourceTask {

    public static final String DATABASE_NAME_FIELD = "dbname";
    public static final String POSITION_FIELD = "position";
    private static final Schema VALUE_SCHEMA = Schema.STRING_SCHEMA;

    private String dbName;
    private String topic;
    private Long dbOffset = 0L;

    @Override
    public String version() {
        return new CustomDBSourceConnector().version();
    }

    @Override
    public void start(Map<String, String> props) {
        AbstractConfig config = new AbstractConfig(CustomDBSourceConnector.CONFIG_DEF, props);
        dbName = config.getString(CustomDBSourceConnector.DB_CONFIG);
        topic = config.getString(CustomDBSourceConnector.TOPIC_CONFIG);
    }

    @Override
    public List<SourceRecord> poll() {

        Map<String, Object> offset = context
                .offsetStorageReader()
                .offset(Collections.singletonMap(DATABASE_NAME_FIELD, dbName));

        if (offset != null) {
            Object lastRecordedOffset = offset.get(POSITION_FIELD);

            if (lastRecordedOffset != null && !(lastRecordedOffset instanceof Long))
                throw new ConnectException("Offset position is the incorrect type");

            if (lastRecordedOffset != null)
                log.debug("Skipped to offset {}", lastRecordedOffset);

            dbOffset = (lastRecordedOffset != null) ? (Long) lastRecordedOffset : 0L;
        } else {
            dbOffset = 0L;
        }

        List<String> transactions = getTransactions(dbOffset);
        return transactions
                .stream()
                .map(this::createSourceRecord)
                .toList();
    }

    @Override
    public void stop() {
    }

    private SourceRecord createSourceRecord(String transaction) {
        return new SourceRecord(
                offsetKey(this.dbName),
                offsetValue(this.dbOffset),
                this.topic,
                null,
                null,
                null,
                VALUE_SCHEMA,
                transaction,
                System.currentTimeMillis()
        );
    }

    private List<String> getTransactions(Long dbOffset) {
        List<String> result = new ArrayList<>();
        try {
            Class.forName("org.h2.Driver");
        } catch (ClassNotFoundException exception) {
            log.error("org.h2.Driver не найден, добавьте зависимость");
            throw new RuntimeException(exception);
        }

        try (var connection = DriverManager.getConnection(getDbUrl());
             PreparedStatement statement = connection.prepareStatement("select * from transaction offset ?")) {
            statement.setLong(1, dbOffset);
            ResultSet resultSet = statement.executeQuery();
            while (resultSet.next()) {
                result.add("id = " + resultSet.getString("id") +
                        " operationType = " + resultSet.getString("operationType") +
                        " amount = " + resultSet.getString("amount") +
                        " account = " + resultSet.getString("account") +
                        " dateOperation = " + resultSet.getString("dateOperation")
                );
            }
        } catch (Exception e) {
            log.error("Ошибка работы с БД");
            throw new RuntimeException(e);
        }

        return result;
    }

    private String getDbUrl() {
        return Config.getProperties()
                .getProperty("dbname");
    }

    private Map<String, String> offsetKey(String filename) {
        return Collections.singletonMap(DATABASE_NAME_FIELD, filename);
    }

    private Map<String, Long> offsetValue(Long pos) {
        return Collections.singletonMap(POSITION_FIELD, pos);
    }

}
