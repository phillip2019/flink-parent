package com.aikosolar.debezium.study;

import io.debezium.data.Envelope;
import io.debezium.embedded.Connect;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.Executors;

/**
 * @author carlc
 */
public class DebeziumTest {

    //  https://debezium.io/documentation/reference/1.2/development/engine.html
    public static void main(String[] args) throws Exception {
        Properties properties = new Properties();
        properties.setProperty("name", "engine");
        properties.setProperty("connector.class", "io.debezium.connector.mysql.MySqlConnector");
        properties.setProperty("offset.storage", "org.apache.kafka.connect.storage.FileOffsetBackingStore");
        properties.setProperty("offset.storage.file.filename", "/tmp/offsets.dat");
        properties.setProperty("offset.flush.interval.ms", "60000");
        properties.setProperty("database.hostname", "localhost");
        properties.setProperty("database.port", "3306");
        properties.setProperty("database.user", "root");
        properties.setProperty("database.password", "123456");
        properties.setProperty("database.server.name", "mysql-binlog-connector");
        properties.setProperty("database.history", "io.debezium.relational.history.FileDatabaseHistory");
        properties.setProperty("database.history.file.filename", "dbhistory.dat");


        DebeziumEngine<ChangeEvent<SourceRecord, SourceRecord>> engine = DebeziumEngine.create(Connect.class)
                .using(properties)

                // 1. 提出ChangeConsumer
                // 2. 独立转换器
                // 3. 字段类型转换
                .notifying((records, committer) -> {
                    for (ChangeEvent<SourceRecord, SourceRecord> r : records) {
                        SourceRecord record = r.value();
                        Struct value = (Struct) record.value();
                        Schema valueSchema = record.valueSchema();
                        Envelope.Operation op = Envelope.operationFor(record);
                        if (op != null) {
                            System.out.println(op.code());
                            switch (op) {
                                case READ:
                                case CREATE: {
                                    Schema afterSchema = valueSchema.field(Envelope.FieldName.AFTER).schema();
                                    Struct after = value.getStruct(Envelope.FieldName.AFTER);
                                    List<Field> ff = afterSchema.fields();
                                    for (Field f : ff) {
                                        System.out.println(String.format("CREATE|%s(%s) = %s", f.name(), f.schema().type(), after.get(f.name())));
                                    }
                                }
                                break;
                                case DELETE: {
                                    Schema beforeSchema = valueSchema.field(Envelope.FieldName.BEFORE).schema();
                                    Struct before = value.getStruct(Envelope.FieldName.BEFORE);
                                    for (Field f : beforeSchema.fields()) {
                                        System.out.println(String.format("DELETE|变更前:%s(%s) = %s", f.name(), f.schema().type(), before.get(f.name())));
                                    }
                                }
                                break;
                                case UPDATE: {
                                    Schema beforeSchema = valueSchema.field(Envelope.FieldName.BEFORE).schema();
                                    Struct before = value.getStruct(Envelope.FieldName.BEFORE);
                                    for (Field f : beforeSchema.fields()) {
                                        System.out.println(String.format("UPDATE|变更前:%s(%s) = %s", f.name(), f.schema().type(), before.get(f.name())));
                                    }

                                    Schema afterSchema = valueSchema.field(Envelope.FieldName.AFTER).schema();
                                    Struct after = value.getStruct(Envelope.FieldName.AFTER);
                                    for (Field f : afterSchema.fields()) {
                                        System.out.println(String.format("UPDATE|变更后:%s(%s) = %s", f.name(), f.schema().type(), after.get(f.name())));
                                    }
                                }
                                break;
                            }

                        }
                        committer.markProcessed(r);
                    }
                })
                .using((success, message, error) -> {
                    // 所有参数都看不懂,先打印吧
                    System.out.println(success);
                    System.out.println(message);
                    System.out.println(error);
                })
                .build();

        Executors.newSingleThreadExecutor().execute(engine);

    }
}