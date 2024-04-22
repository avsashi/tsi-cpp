/*
 * Copyright (C) ActiveViam 2023-2024
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam Limited. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */
package com.activeviam.apps.cfg;

import static com.activeviam.apps.cfg.ApplicationConfig.START_MANAGER;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;
import org.springframework.core.env.Environment;

import com.activeviam.apps.constants.StoreAndFieldConstants;
import com.qfs.gui.impl.JungSchemaPrinter;
import com.qfs.msg.IMessageChannel;
import com.qfs.msg.csv.ICSVParserConfiguration;
import com.qfs.msg.csv.IFileInfo;
import com.qfs.msg.csv.ILineReader;
import com.qfs.msg.csv.filesystem.impl.FileSystemCSVTopicFactory;
import com.qfs.msg.csv.impl.CSVParserConfiguration;
import com.qfs.msg.csv.impl.CSVSource;
import com.qfs.msg.csv.impl.CSVSourceConfiguration;
import com.qfs.platform.IPlatform;
import com.qfs.source.impl.CSVMessageChannelFactory;
import com.qfs.store.IDatastore;
import com.qfs.store.NoTransactionException;
import com.qfs.store.transaction.DatastoreTransactionException;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Configuration
@RequiredArgsConstructor
@Slf4j
public class SourceConfig {
    public static final String INITIAL_LOAD = "initialLoad";

    public static final String POSITIONS_TOPIC = "Positions";
    public static final String CCP_TOPIC = "Ccp";
    public static final String VAR_TOPIC = "Var";

    private final Environment env;
    private final IDatastore datastore;

    @Bean
    @DependsOn(INITIAL_LOAD)
    public void showDatastoreSchema() {
        new JungSchemaPrinter(false).print("Training datastore", datastore);
    }

    /**
     * Topic factory bean. Allows to create CSV topics and watch changes to directories. Autocloseable.
     *
     * @return the topic factory
     */
    @Bean
    public FileSystemCSVTopicFactory csvTopicFactory() {
        return new FileSystemCSVTopicFactory(false);
    }

    @Bean(destroyMethod = "close")
    public CSVSource<Path> csvSource() {
        var schemaMetadata = datastore.getQueryMetadata().getMetadata();
        var csvTopicFactory = csvTopicFactory();
        var csvSource = new CSVSource<Path>();

        // Adding Positions Topic
        var positionsColumns = schemaMetadata.getFields(StoreAndFieldConstants.POSITIONS);
        var positionsTopic = csvTopicFactory.createTopic(
                POSITIONS_TOPIC,
                env.getProperty("file.positions"),
                createParserConfig(positionsColumns.size(), positionsColumns));
        csvSource.addTopic(positionsTopic);
        logTopicAdded(POSITIONS_TOPIC);

        // Adding CCP Topic
        var ccpColumns = schemaMetadata.getFields(StoreAndFieldConstants.CCP);
        var ccpTopic = csvTopicFactory.createTopic(
                CCP_TOPIC, env.getProperty("file.ccp"), createParserConfig(positionsColumns.size(), ccpColumns));
        csvSource.addTopic(ccpTopic);
        logTopicAdded(CCP_TOPIC);

        // Adding VAR Topic
        var varColumns = schemaMetadata.getFields(StoreAndFieldConstants.VAR);
        var varTopic = csvTopicFactory.createTopic(
                VAR_TOPIC, env.getProperty("file.var"), createParserConfig(positionsColumns.size(), varColumns));
        csvSource.addTopic(varTopic);
        logTopicAdded(VAR_TOPIC);

        // Allocate half the machine cores to CSV parsing
        var parserThreads = Math.max(2, IPlatform.CURRENT_PLATFORM.getProcessorCount() / 2);
        log.info("Allocating " + parserThreads + " parser threads.");

        var sourceConfigurationBuilder = new CSVSourceConfiguration.CSVSourceConfigurationBuilder<Path>();
        sourceConfigurationBuilder.parserThreads(parserThreads);
        sourceConfigurationBuilder.synchronousMode(Boolean.valueOf(env.getProperty("synchronousMode", "false")));
        csvSource.configure(sourceConfigurationBuilder.build());
        return csvSource;
    }

    private void logTopicAdded(String topicName) {
        log.info("Topic: [{}] added to csvSource.", topicName);
    }

    @Bean
    public CSVMessageChannelFactory<Path> csvChannelFactory() {
        return new CSVMessageChannelFactory<>(csvSource(), datastore);
    }

    @Bean(INITIAL_LOAD)
    @DependsOn(START_MANAGER)
    public Void initialLoad() {
        log.info("Initial load starting.");
        // csv
        Collection<IMessageChannel<IFileInfo<Path>, ILineReader>> csvChannels = new ArrayList<>();
        csvChannels.add(csvChannelFactory().createChannel(POSITIONS_TOPIC, StoreAndFieldConstants.POSITIONS));
        csvChannels.add(csvChannelFactory().createChannel(CCP_TOPIC, StoreAndFieldConstants.CCP));
        csvChannels.add(csvChannelFactory().createChannel(VAR_TOPIC, StoreAndFieldConstants.VAR));

        // do the transactions
        var before = System.nanoTime();
        var transactionManager = datastore.getTransactionManager();

        // start transaction
        try {
            log.info("Starting transaction.");
            transactionManager.startTransaction();
            log.info("Fetching the sources.");
            csvSource().fetch(csvChannels);
            log.info("Committing transaction.");
            transactionManager.commitTransaction();
            log.info("Transaction committed.");
        } catch (DatastoreTransactionException | NoTransactionException e) {
            log.error("An error occurred during the commit of the transaction.", e);
        }

        var elapsed = System.nanoTime() - before;
        log.info("Initial data load completed in [{}] ms.", elapsed / 1000000L);
        return null;
    }

    private ICSVParserConfiguration createParserConfig(int columnCount, List<String> columns) {
        var cfg = columns == null ? new CSVParserConfiguration(columnCount) : new CSVParserConfiguration(columns);
        cfg.setNumberSkippedLines(1); // skip the first line
        return cfg;
    }
}
