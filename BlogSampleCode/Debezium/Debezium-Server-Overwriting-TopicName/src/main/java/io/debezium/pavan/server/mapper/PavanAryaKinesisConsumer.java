/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.kinesis;

import java.util.List;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.enterprise.context.Dependent;
import javax.enterprise.inject.Instance;
import javax.inject.Inject;
import javax.inject.Named;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.ConfigProvider;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.DebeziumEngine.RecordCommitter;
import io.debezium.server.BaseChangeConsumer;
import io.debezium.server.CustomConsumerBuilder;

import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.PutRecordRequest;

/**
 * Implementation of the consumer that delivers the messages into Amazon Kinesis destination.
 *
 * @author Pavan Kumar Aryasomayajulu
 *
 */
@Named("pavankinesis")
@Dependent
public class PavanAryaKinesisConsumer extends BaseChangeConsumer implements DebeziumEngine.ChangeConsumer<ChangeEvent<Object, Object>> {

    private static final Logger LOGGER = LoggerFactory.getLogger(PavanAryaKinesisConsumer.class);

    private static final String PROP_PREFIX = "debezium.sink.kinesis.";
    private static final String PROP_REGION_NAME = PROP_PREFIX + "region";

    private String region;

    @ConfigProperty(name = PROP_PREFIX + "credentials.profile", defaultValue = "default")
    String credentialsProfile;

    @ConfigProperty(name = PROP_PREFIX + "null.key", defaultValue = "default")
    String nullKey;
	
	@ConfigProperty(name = PROP_PREFIX + "kinesis.error.log", defaultValue = "kinesis_error.txt")
    String errorLogFile;

    private KinesisClient client = null;

    @Inject
    @CustomConsumerBuilder
    Instance<KinesisClient> customClient;

    @PostConstruct
    void connect() {
        if (customClient.isResolvable()) {
            client = customClient.get();
            LOGGER.info("Obtained custom configured KinesisClient '{}'", client);
            return;
        }

        final Config config = ConfigProvider.getConfig();
        region = config.getValue(PROP_REGION_NAME, String.class);
        client = KinesisClient.builder()
                .region(Region.of(region))
                .credentialsProvider(DefaultCredentialsProvider.create())
                .build();
        LOGGER.info("Using default KinesisClient_Pavan '{}'", client);
    }

    @PreDestroy
    void close() {
        try {
            client.close();
        }
        catch (Exception e) {
            LOGGER.warn("Exception while closing Kinesis client: {}", e);
        }
    }

    @Override
    public void handleBatch(List<ChangeEvent<Object, Object>> records, RecordCommitter<ChangeEvent<Object, Object>> committer)
            throws InterruptedException {
        for (ChangeEvent<Object, Object> record : records) {
			try{
				LOGGER.trace("Received event '{}'", record);
				final PutRecordRequest putRecord = PutRecordRequest.builder()
						.partitionKey((record.key() != null) ? getString(record.key()) : nullKey)
						.streamName(streamNameMapper.map(record.destination()))
						.data(SdkBytes.fromByteArray(getBytes(record.value())))
						.build();
				client.putRecord(putRecord);
			}
			catch (Exception e) {
				LOGGER.info("Received event '{}'", record);
				LOGGER.warn("Exception while putting Kinesis record: {}", e);
				appendUsingFileWriter(errorLogFile,"Received event "+record.toString());
				appendUsingFileWriter(errorLogFile,"Exception while putting Kinesis record: "+e.toString());
				appendUsingFileWriter(errorLogFile,"=======================================");
			}
			try{
				committer.markProcessed(record);
			}
			catch (Exception e) {
				LOGGER.warn("Exception while comitting Kinesis record: {}", e);
			}
        }
        committer.markBatchFinished();
    }
	
	private void appendUsingFileWriter(String filePath, String text) {
		File file = new File(filePath);
		FileWriter fr = null;
		try {
			if (file.exists())
			{
			   fr = new FileWriter(file,true);//if file exists append to file. Works fine.
			}
			else
			{
			   file.createNewFile();
			   fr = new FileWriter(file);
			}
			// Below constructor argument decides whether to append or override
			//fr = new FileWriter(file, true);
			fr.write(text);

		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				fr.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
}