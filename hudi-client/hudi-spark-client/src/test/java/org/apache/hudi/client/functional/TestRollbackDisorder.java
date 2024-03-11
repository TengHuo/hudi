/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.client.functional;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieInstant.State;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion;
import org.apache.hudi.common.table.view.FileSystemViewManager;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.table.view.FileSystemViewStorageType;
import org.apache.hudi.common.table.view.RemoteHoodieTableFileSystemView;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.metadata.HoodieBackedTestDelayedTableMetadata;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.testutils.HoodieClientTestBase;
import org.apache.hudi.timeline.service.TimelineService;
import org.apache.spark.api.java.JavaRDD;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.hudi.common.table.timeline.HoodieTimeline.COMMIT_ACTION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.ROLLBACK_ACTION;
import static org.apache.hudi.common.table.view.FileSystemViewStorageConfig.REMOTE_PORT_NUM;

/**
 * Tests the {@link RemoteHoodieTableFileSystemView}
 * Note: This is a class internal to Shopee.
 */
@Tag("functional")
public class TestRollbackDisorder extends HoodieClientTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(TestRollbackDisorder.class);

  @BeforeEach
  public void setUp() throws Exception {
    initPath();
    initSparkContexts();
    initFileSystem();
    initMetaClient();
    initTimelineService();
    dataGen = new HoodieTestDataGenerator(0x1f86);
  }

  @AfterEach
  public void tearDown() throws Exception {
    cleanupTimelineService();
    cleanupClients();
    cleanupSparkContexts();
    cleanupFileSystem();
    cleanupExecutorService();
    dataGen.close();
    dataGen = null;
    System.gc();
  }

  @Override
  public void initTimelineService() {
    // Start a timeline server that are running across multiple commits
    HoodieLocalEngineContext localEngineContext = new HoodieLocalEngineContext(metaClient.getHadoopConf());

    try {
      HoodieWriteConfig config = HoodieWriteConfig.newBuilder()
          .withPath(basePath)
          .withFileSystemViewConfig(FileSystemViewStorageConfig.newBuilder()
              .withRemoteServerPort(incrementTimelineServicePortToUse()).build())
          .build();
      timelineService = new TimelineService(localEngineContext, new Configuration(),
          TimelineService.Config.builder().enableMarkerRequests(true)
              .serverPort(config.getViewStorageConfig().getRemoteViewServerPort()).build(),
          FileSystem.get(new Configuration()),
          FileSystemViewManager.createViewManager(
              context, config.getMetadataConfig(), config.getViewStorageConfig(),
              config.getCommonConfig(),
              metaClient -> new HoodieBackedTestDelayedTableMetadata(
                  context, config.getMetadataConfig(), metaClient.getBasePathV2().toString(), true)));
      timelineService.startService();
      timelineServicePort = timelineService.getServerPort();
      LOG.info("Started timeline server on port: " + timelineServicePort);
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  @Test
  public void testNewInstantTime() {
    String instantTime = HoodieActiveTimeline.formatDate(new Date());
    System.out.println("instantTime: " + instantTime);
  }

  @Test
  public void testRollbackDisorderIssue() throws IOException {
    SparkRDDWriteClient writeClient1 = createWriteClient(Option.of(timelineService));

    // Create 3 commits
    for (int i = 0; i < 3; i++) {
      writeToTable(i, writeClient1);
    }

    // Create a newMetaClient for checking timeline states
    HoodieTableMetaClient newMetaClient = HoodieTableMetaClient.builder()
        .setConf(metaClient.getHadoopConf())
        .setBasePath(basePath)
        .build();
    HoodieActiveTimeline activeTimeline = newMetaClient.getActiveTimeline();
    System.out.println("Active timeline1: " + activeTimeline.toString());
    Assertions.assertEquals(3, activeTimeline.getInstants().size(), "There should be 3 commits in active timeline");
    activeTimeline.getInstants().forEach(instant -> {
      Assertions.assertEquals(COMMIT_ACTION, instant.getAction());
      Assertions.assertEquals(State.COMPLETED, instant.getState());
    });

    // generate a empty write request
    String emptyWriteInstantTime = HoodieActiveTimeline.formatDate(new Date());
    HoodieInstant emptyWriteInstant = new HoodieInstant(State.REQUESTED, HoodieTimeline.COMMIT_ACTION, emptyWriteInstantTime);
    activeTimeline.createNewInstant(emptyWriteInstant);
    activeTimeline.transitionRequestedToInflight(COMMIT_ACTION, emptyWriteInstantTime);
    newMetaClient.reloadActiveTimeline();
    activeTimeline = newMetaClient.getActiveTimeline();
    System.out.println("Active timeline2: " + activeTimeline.toString());
    Assertions.assertEquals(4, activeTimeline.getInstants().size(), "There should be 4 commits in active timeline");
    Option<HoodieInstant> inflightInstant = activeTimeline.lastInstant();
    Assertions.assertTrue(inflightInstant.isPresent());
    Assertions.assertEquals(COMMIT_ACTION, inflightInstant.get().getAction());
    Assertions.assertEquals(State.INFLIGHT, inflightInstant.get().getState());

    // write again, should do a rollback
    String instantTime1 = HoodieActiveTimeline.formatDate(new Date());
    writeClient1.startCommitWithTime(instantTime1);
    List<HoodieRecord> records1 = dataGen.generateInserts(instantTime1, 100);
    JavaRDD<WriteStatus> writeStatusRDD1 = writeClient1.upsert(jsc.parallelize(records1, 1), instantTime1);
    writeClient1.commit(instantTime1, writeStatusRDD1, Option.empty(), COMMIT_ACTION, Collections.emptyMap());

    // check timeline
    newMetaClient.reloadActiveTimeline();
    activeTimeline = newMetaClient.getActiveTimeline();
    System.out.println("Active timeline3: " + activeTimeline.toString());
    Assertions.assertEquals(5, activeTimeline.getInstants().size(), "There should be 4 commits and 1 rollback in active timeline");
    Option<HoodieInstant> rollbackInstant = activeTimeline.lastInstant();
    Assertions.assertTrue(rollbackInstant.isPresent());
    Assertions.assertEquals(ROLLBACK_ACTION, rollbackInstant.get().getAction());
    Assertions.assertEquals(State.COMPLETED, rollbackInstant.get().getState());
    activeTimeline.getInstants().forEach(instant -> Assertions.assertEquals(State.COMPLETED, instant.getState()));

    Option<HoodieInstant> latestCommit = activeTimeline.getCommitsTimeline().lastInstant();
    Assertions.assertTrue(latestCommit.isPresent());
    Assertions.assertEquals(COMMIT_ACTION, latestCommit.get().getAction());
    Assertions.assertEquals(State.COMPLETED, latestCommit.get().getState());

    // Check getLatestBaseFile, test will fail
//    HoodieTable hoodieTable = writeClient1.initTable(WriteOperationType.UPSERT, Option.empty());
//    List<HoodieBaseFile> latestBaseFiles = hoodieTable.getBaseFileOnlyView().getLatestBaseFiles().collect(Collectors.toList());
//    Optional<HoodieBaseFile> oneBaseFile = hoodieTable.getBaseFileOnlyView().getLatestBaseFiles().findFirst();
//    Assertions.assertTrue(oneBaseFile.isPresent(), "Find a random base file, should exist");
//    String partitionPath = oneBaseFile.get().getPath().replace("file:" + basePath + "/", "").replace(oneBaseFile.get().getFileName(), "");
//    Option<HoodieBaseFile> latestBaseFile = hoodieTable.getBaseFileOnlyView().getLatestBaseFile(partitionPath, oneBaseFile.get().getFileId());
//    Assertions.assertTrue(latestBaseFile.isPresent());
//    System.out.println("Latest file path: " + latestBaseFile.get().getPath());
//    System.out.println("Active timeline for getLatestBaseFile check: " + activeTimeline.toString());
//    Assertions.assertEquals(latestCommit.get().getTimestamp(), latestBaseFile.get().getCommitTime());

    // write again, will ignore the previous batch
    String instantTime2 = HoodieActiveTimeline.formatDate(new Date());
    writeClient1.startCommitWithTime(instantTime2);
    List<HoodieRecord> records2 = dataGen.generateInserts(instantTime2, 100);
    JavaRDD<WriteStatus> writeStatusRDD2 = writeClient1.upsert(jsc.parallelize(records2, 1), instantTime2);
    writeClient1.commit(instantTime2, writeStatusRDD2, Option.empty(), COMMIT_ACTION, Collections.emptyMap());

    newMetaClient.reloadActiveTimeline();
    activeTimeline = newMetaClient.getActiveTimeline();
    System.out.println("Active timeline4: " + activeTimeline.toString());
  }

  @Override
  protected HoodieTableType getTableType() {
    return HoodieTableType.COPY_ON_WRITE;
  }

  private SparkRDDWriteClient createWriteClient(Option<TimelineService> timelineService) {
    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder()
        .withPath(basePath)
        .withSchema(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA)
        .withParallelism(2, 2)
        .withBulkInsertParallelism(2)
        .withFinalizeWriteParallelism(2)
        .withDeleteParallelism(2)
        .withTimelineLayoutVersion(TimelineLayoutVersion.CURR_VERSION)
        .withMergeSmallFileGroupCandidatesLimit(0)
        .withFileSystemViewConfig(FileSystemViewStorageConfig.newBuilder()
            .withStorageType(FileSystemViewStorageType.REMOTE_ONLY)
            .withRemoteServerPort(timelineService.isPresent()
                ? timelineService.get().getServerPort() : REMOTE_PORT_NUM.defaultValue())
            .build())
        .withAutoCommit(false)
        .forTable("test_mor_table")
        .build();
    return new SparkRDDWriteClient(context, writeConfig, timelineService);
  }

  private void writeToTable(int round, SparkRDDWriteClient writeClient) throws IOException {
    String instantTime = HoodieActiveTimeline.formatDate(new Date());
    writeClient.startCommitWithTime(instantTime);
    List<HoodieRecord> records = round == 0
        ? dataGen.generateInserts(instantTime, 100)
        : dataGen.generateUpdates(instantTime, 100);

    JavaRDD<WriteStatus> writeStatusRDD = writeClient.upsert(jsc.parallelize(records, 1), instantTime);
    writeClient.commit(instantTime, writeStatusRDD, Option.empty(), COMMIT_ACTION, Collections.emptyMap());
  }
}
