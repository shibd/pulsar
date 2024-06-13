/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.client.api;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.mledger.impl.ManagedCursorImpl;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.PulsarChannelInitializer;
import org.apache.pulsar.broker.service.ServerCnx;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.impl.ConsumerImpl;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.common.api.proto.CommandFlow;
import org.apache.pulsar.common.policies.data.ManagedLedgerInternalStats;
import org.apache.pulsar.common.policies.data.PersistentTopicInternalStats;
import org.apache.pulsar.common.policies.data.SubscriptionStats;
import org.apache.pulsar.common.policies.data.TopicStats;
import org.awaitility.Awaitility;
import org.awaitility.reflect.WhiteboxImpl;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker-api")
@Slf4j
public class NonDurableSubscriptionTest extends ProducerConsumerBase {

    private final AtomicInteger numFlow = new AtomicInteger(0);

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        conf.setTopicLevelPoliciesEnabled(false);
        conf.setSystemTopicEnabled(false);
        conf.setSubscriptionExpirationTimeMinutes(1);
        super.internalSetup();
        super.producerBaseSetup();
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Override
    protected BrokerService customizeNewBrokerService(BrokerService brokerService) {
        brokerService.setPulsarChannelInitializerFactory((_pulsar, opts) -> {
            return new PulsarChannelInitializer(_pulsar, opts) {
                @Override
                protected ServerCnx newServerCnx(PulsarService pulsar, String listenerName) throws Exception {
                    return new ServerCnx(pulsar) {

                        @Override
                        protected void handleFlow(CommandFlow flow) {
                            super.handleFlow(flow);
                            numFlow.incrementAndGet();
                        }
                    };
                }
            };
        });
        return brokerService;
    }

    @Test
    public void testNonDurableSubscription() throws Exception {
        String topicName = "persistent://my-property/my-ns/nonDurable-topic1";
        // 1 setup producer、consumer
        @Cleanup
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING).topic(topicName)
                .create();
        @Cleanup
        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING).topic(topicName)
                .readCompacted(true)
                .subscriptionMode(SubscriptionMode.NonDurable)
                .subscriptionType(SubscriptionType.Exclusive)
                .subscriptionName("my-nonDurable-subscriber")
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscribe();
        // 2 send message
        int messageNum = 10;
        for (int i = 0; i < messageNum; i++) {
            producer.send("message" + i);
        }
        // 3 receive the first 5 messages
        for (int i = 0; i < 5; i++) {
            Message<String> message = consumer.receive();
            assertNotNull(message);
            Assert.assertEquals(message.getValue(), "message" + i);
            consumer.acknowledge(message);
        }
        // 4 trigger reconnect
        ((ConsumerImpl)consumer).getClientCnx().close();
        // 5 for non-durable we are going to restart from the next entry
        for (int i = 5; i < messageNum; i++) {
            Message<String> message = consumer.receive();
            assertNotNull(message);
            Assert.assertEquals(message.getValue(), "message" + i);
        }

    }

    @Test
    public void testSameSubscriptionNameForDurableAndNonDurableSubscription() throws Exception {
        String topicName = "persistent://my-property/my-ns/same-sub-name-topic";
        // first test for create Durable subscription and then create NonDurable subscription
        // 1. create a subscription with SubscriptionMode.Durable
        @Cleanup
        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING).topic(topicName)
                .readCompacted(true)
                .subscriptionMode(SubscriptionMode.Durable)
                .subscriptionType(SubscriptionType.Exclusive)
                .subscriptionName("mix-subscription")
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscribe();
        consumer.close();

        // 2. create a subscription with SubscriptionMode.NonDurable
        try {
            @Cleanup
            Consumer<String> consumerNoDurable =
                    pulsarClient.newConsumer(Schema.STRING).topic(topicName)
                    .readCompacted(true)
                    .subscriptionMode(SubscriptionMode.NonDurable)
                    .subscriptionType(SubscriptionType.Exclusive)
                    .subscriptionName("mix-subscription")
                    .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                    .subscribe();
            Assert.fail("should fail since durable subscription already exist.");
        } catch (PulsarClientException.NotAllowedException exception) {
            //ignore
        }

        // second test for create NonDurable subscription and then create Durable subscription
        @Cleanup
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING).topic(topicName)
                .create();
        // 1. create a subscription with SubscriptionMode.NonDurable
        @Cleanup
        Consumer<String> noDurableConsumer =
                pulsarClient.newConsumer(Schema.STRING).topic(topicName)
                        .subscriptionMode(SubscriptionMode.NonDurable)
                        .subscriptionType(SubscriptionType.Shared)
                        .subscriptionName("mix-subscription-01")
                        .receiverQueueSize(1)
                        .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                        .subscribe();

        // 2. create a subscription with SubscriptionMode.Durable
        try {
            @Cleanup
            Consumer<String> durableConsumer = pulsarClient.newConsumer(Schema.STRING).topic(topicName)
                    .subscriptionMode(SubscriptionMode.Durable)
                    .subscriptionType(SubscriptionType.Shared)
                    .subscriptionName("mix-subscription-01")
                    .receiverQueueSize(1)
                    .startMessageIdInclusive()
                    .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                    .subscribe();
        } catch (PulsarClientException.NotAllowedException exception) {
            //ignore
        }
    }

    @DataProvider(name = "subscriptionTypes")
    public static Object[][] subscriptionTypes() {
        Object[][] result = new Object[SubscriptionType.values().length][];
        int i = 0;
        for (SubscriptionType type : SubscriptionType.values()) {
            result[i++] = new Object[] {type};
        }
        return result;
    }

    @Test(dataProvider = "subscriptionTypes")
    public void testNonDurableSubscriptionRecovery(SubscriptionType subscriptionType) throws Exception {
        log.info("testing {}", subscriptionType);
        String topicName = "persistent://my-property/my-ns/nonDurable-sub-recorvery-"+subscriptionType;
        // 1 setup producer、consumer
        @Cleanup
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING).topic(topicName)
                .create();
        @Cleanup
        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING).topic(topicName)
                .subscriptionMode(SubscriptionMode.NonDurable)
                .subscriptionType(subscriptionType)
                .subscriptionName("my-nonDurable-subscriber")
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscribe();
        // 2 send messages
        int messageNum = 15;
        for (int i = 0; i < messageNum; i++) {
            producer.send("message" + i);
        }
        // 3 receive the first 5 messages
        for (int i = 0; i < 5; i++) {
            Message<String> message = consumer.receive();
            assertNotNull(message);
            Assert.assertEquals(message.getValue(), "message" + i);
            consumer.acknowledge(message);
        }
        // 4 trigger reconnect
        ((ConsumerImpl)consumer).getClientCnx().close();

        // 5 for non-durable we are going to restart from the next entry
        for (int i = 5; i < 10; i++) {
            Message<String> message = consumer.receive();
            assertNotNull(message);
            Assert.assertEquals(message.getValue(), "message" + i);
        }

        // 6 restart broker
        restartBroker();

        // 7 for non-durable we are going to restart from the next entry
        for (int i = 10; i < messageNum; i++) {
            Message<String> message = consumer.receive();
            assertNotNull(message);
            Assert.assertEquals(message.getValue(), "message" + i);
        }

    }

    @Test
    public void testFlowCountForMultiTopics() throws Exception {
        String topicName = "persistent://my-property/my-ns/test-flow-count";
        int numPartitions = 5;
        admin.topics().createPartitionedTopic(topicName, numPartitions);
        numFlow.set(0);

        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic(topicName)
                .subscriptionName("my-nonDurable-subscriber")
                .subscriptionMode(SubscriptionMode.NonDurable)
                .subscribe();
        consumer.receive(1, TimeUnit.SECONDS);
        consumer.close();

        assertEquals(numFlow.get(), numPartitions);
    }

    private void trimLedgers(final String tpName) {
        // Wait for topic loading.
        org.awaitility.Awaitility.await().untilAsserted(() -> {
            PersistentTopic persistentTopic =
                    (PersistentTopic) pulsar.getBrokerService().getTopic(tpName, false).join().get();
            assertNotNull(persistentTopic);
        });
        PersistentTopic persistentTopic =
                (PersistentTopic) pulsar.getBrokerService().getTopic(tpName, false).join().get();
        ManagedLedgerImpl ml = (ManagedLedgerImpl) persistentTopic.getManagedLedger();
        CompletableFuture<Void> trimLedgersTask = new CompletableFuture<>();
        ml.trimConsumedLedgersInBackground(trimLedgersTask);
        trimLedgersTask.join();
    }

    private void switchLedgerManually(final String tpName) throws Exception {
        Method ledgerClosed =
                ManagedLedgerImpl.class.getDeclaredMethod("ledgerClosed", new Class[]{LedgerHandle.class});
        Method createLedgerAfterClosed =
                ManagedLedgerImpl.class.getDeclaredMethod("createLedgerAfterClosed", new Class[0]);
        ledgerClosed.setAccessible(true);
        createLedgerAfterClosed.setAccessible(true);

        // Wait for topic create.
        org.awaitility.Awaitility.await().untilAsserted(() -> {
            PersistentTopic persistentTopic =
                    (PersistentTopic) pulsar.getBrokerService().getTopic(tpName, false).join().get();
            assertNotNull(persistentTopic);
        });

        // Switch ledger.
        PersistentTopic persistentTopic =
                (PersistentTopic) pulsar.getBrokerService().getTopic(tpName, false).join().get();
        ManagedLedgerImpl ml = (ManagedLedgerImpl) persistentTopic.getManagedLedger();
        LedgerHandle currentLedger1 = WhiteboxImpl.getInternalState(ml, "currentLedger");
        ledgerClosed.invoke(ml, new Object[]{currentLedger1});
        createLedgerAfterClosed.invoke(ml, new Object[0]);
        Awaitility.await().untilAsserted(() -> {
            LedgerHandle currentLedger2 = WhiteboxImpl.getInternalState(ml, "currentLedger");
            assertNotEquals(currentLedger1.getId(), currentLedger2.getId());
        });
    }

    @Test
    public void testHasMessageAvailableIfIncomingQueueNotEmpty() throws Exception {
        final String nonDurableCursor = "non-durable-cursor";
        final String topicName = BrokerTestUtil.newUniqueName("persistent://public/default/tp");
        Reader<String> reader = pulsarClient.newReader(Schema.STRING).topic(topicName).receiverQueueSize(1)
                .subscriptionName(nonDurableCursor).startMessageId(MessageIdImpl.earliest).create();
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING).topic(topicName).create();
        MessageIdImpl msgSent = (MessageIdImpl) producer.send("1");

        // Trigger switch ledger.
        // Trigger a trim ledgers task, and verify trim ledgers successful.
        switchLedgerManually(topicName);
        trimLedgers(topicName);

        // Since there is one message in the incoming queue, so the method "reader.hasMessageAvailable" should return
        // true.
        boolean hasMessageAvailable = reader.hasMessageAvailable();
        Message<String> msgReceived = reader.readNext(2, TimeUnit.SECONDS);
        if (msgReceived == null) {
            assertFalse(hasMessageAvailable);
        } else {
            log.info("receive msg: {}", msgReceived.getValue());
            assertTrue(hasMessageAvailable);
            assertEquals(msgReceived.getValue(), "1");
        }

        // cleanup.
        reader.close();
        producer.close();
        admin.topics().delete(topicName);
    }

    @Test(invocationCount = 100)
    public void testInitReaderAtSpecifiedPosition() throws Exception {
        String topicName = BrokerTestUtil.newUniqueName("persistent://public/default/tp");
        admin.topics().createNonPartitionedTopic(topicName);
        admin.topics().createSubscription(topicName, "s0", MessageId.earliest);

        // Trigger 5 ledgers.
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING).topic(topicName).create();
        for (int i = 0; i < 5; i++) {
            MessageIdImpl msgId = (MessageIdImpl) producer.send("1");
            admin.topics().unload(topicName);
        }

        // Less than the first ledger, and entry id is "-1".
        Reader<String> reader2 = pulsarClient.newReader(Schema.STRING).topic(topicName).subscriptionName("s2")
                .readCompacted(true).startMessageId(MessageId.earliest).create();
        for (int i = 0; i < 500; i++) {
            producer.send("1");
        }
        admin.topics().unload(topicName);
        producer.close();

        int i = 0;
        while (reader2.hasMessageAvailable()) {
            System.out.println("debug----0: " + i);
            Message<String> stringMessage = reader2.readNext();
            System.out.println(i++);
        }
        assertEquals(i, 505);

        Awaitility.await().ignoreExceptions().atMost(10, TimeUnit.SECONDS)
                .pollInterval(500, TimeUnit.MILLISECONDS)
                .pollDelay(0, TimeUnit.MILLISECONDS)
                // assert first create topic timeout
                .untilAsserted(() -> {
                    PersistentTopicInternalStats internalStats = admin.topics().getInternalStats(topicName);
                    TopicStats stats = admin.topics().getStats(topicName);
                    SubscriptionStats s2 = stats.getSubscriptions().get("s2");
                    ManagedLedgerInternalStats.CursorStats s2c = internalStats.cursors.get("s2");
                    System.out.println("debug0: backlog: " + s2.getMsgBacklog());
                    System.out.println("debug1: entriesAddedCounter: " + internalStats.entriesAddedCounter);
                    System.out.println("debug1: lastConfirmedEntry: " + internalStats.lastConfirmedEntry);
                    System.out.println("debug1: messagesConsumedCounter: " + s2c.messagesConsumedCounter);
                    System.out.println("debug1: markDeletePosition: " + s2c.markDeletePosition);
                    assertEquals(0, s2.getMsgBacklog());
                    assertEquals(0, internalStats.entriesAddedCounter - s2c.messagesConsumedCounter);
                });

        reader2.close();

        // cleanup
        admin.topics().delete(topicName, false);
    }

    private PositionImpl parseReadPosition(ManagedLedgerInternalStats.CursorStats cursorStats) {
        String[] ledgerIdAndEntryId = cursorStats.readPosition.split(":");
        return PositionImpl.get(Long.valueOf(ledgerIdAndEntryId[0]), Long.valueOf(ledgerIdAndEntryId[1]));
    }

    @Test
    public void testReaderInitAtDeletedPosition() throws Exception {
        String topicName = BrokerTestUtil.newUniqueName("persistent://public/default/tp");
        admin.topics().createNonPartitionedTopic(topicName);
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING).topic(topicName).create();
        producer.send("1");
        producer.send("2");
        producer.send("3");
        MessageIdImpl msgIdInDeletedLedger4 = (MessageIdImpl) producer.send("4");
        MessageIdImpl msgIdInDeletedLedger5 = (MessageIdImpl) producer.send("5");

        // Trigger a trim ledgers task, and verify trim ledgers successful.
        admin.topics().unload(topicName);
        trimLedgers(topicName);
        List<ManagedLedgerInternalStats.LedgerInfo> ledgers = admin.topics().getInternalStats(topicName).ledgers;
        assertEquals(ledgers.size(), 1);
        assertNotEquals(ledgers.get(0).ledgerId, msgIdInDeletedLedger5.getLedgerId());

        // Start a reader at a deleted ledger.
        MessageIdImpl startMessageId =
                new MessageIdImpl(msgIdInDeletedLedger4.getLedgerId(), msgIdInDeletedLedger4.getEntryId(), -1);
        Reader<String> reader = pulsarClient.newReader(Schema.STRING).topic(topicName).subscriptionName("s1")
                .startMessageId(startMessageId).create();
        Message<String> msg1 = reader.readNext(2, TimeUnit.SECONDS);
        Assert.assertNull(msg1);

        // Verify backlog and markDeletePosition is correct.
        Awaitility.await().untilAsserted(() -> {
            SubscriptionStats subscriptionStats = admin.topics()
                    .getStats(topicName, true, true, true).getSubscriptions().get("s1");
            log.info("backlog size: {}", subscriptionStats.getMsgBacklog());
            assertEquals(subscriptionStats.getMsgBacklog(), 0);
            ManagedLedgerInternalStats.CursorStats cursorStats =
                    admin.topics().getInternalStats(topicName).cursors.get("s1");
            String[] ledgerIdAndEntryId = cursorStats.markDeletePosition.split(":");
            PositionImpl actMarkDeletedPos =
                    PositionImpl.get(Long.valueOf(ledgerIdAndEntryId[0]), Long.valueOf(ledgerIdAndEntryId[1]));
            PositionImpl expectedMarkDeletedPos =
                    PositionImpl.get(msgIdInDeletedLedger5.getLedgerId(), msgIdInDeletedLedger5.getEntryId());
            log.info("Expected mark deleted position: {}", expectedMarkDeletedPos);
            log.info("Actual mark deleted position: {}", cursorStats.markDeletePosition);
            assertTrue(actMarkDeletedPos.compareTo(expectedMarkDeletedPos) >= 0);
        });

        // cleanup.
        reader.close();
        producer.close();
        admin.topics().delete(topicName, false);
    }

    @Test
    public void testTrimLedgerIfNoDurableCursor() throws Exception {
        final String nonDurableCursor = "non-durable-cursor";
        final String durableCursor = "durable-cursor";
        final String topicName = BrokerTestUtil.newUniqueName("persistent://public/default/tp");
        admin.topics().createNonPartitionedTopic(topicName);
        Reader<String> reader = pulsarClient.newReader(Schema.STRING).topic(topicName).receiverQueueSize(1)
                .subscriptionName(nonDurableCursor).startMessageId(MessageIdImpl.earliest).create();
        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING).topic(topicName).receiverQueueSize(1)
                .subscriptionName(durableCursor).subscribe();
        consumer.close();

        Producer<String> producer = pulsarClient.newProducer(Schema.STRING).topic(topicName).create();
        producer.send("1");
        producer.send("2");
        producer.send("3");
        producer.send("4");
        MessageIdImpl msgIdInDeletedLedger5 = (MessageIdImpl) producer.send("5");

        Message<String> msg1 = reader.readNext(2, TimeUnit.SECONDS);
        assertEquals(msg1.getValue(), "1");
        Message<String> msg2 = reader.readNext(2, TimeUnit.SECONDS);
        assertEquals(msg2.getValue(), "2");
        Message<String> msg3 = reader.readNext(2, TimeUnit.SECONDS);
        assertEquals(msg3.getValue(), "3");

        // Unsubscribe durable cursor.
        // Trigger a trim ledgers task, and verify trim ledgers successful.
        admin.topics().unload(topicName);
        Thread.sleep(3 * 1000);
        admin.topics().deleteSubscription(topicName, durableCursor);
        // Trim ledgers after release durable cursor.
        trimLedgers(topicName);
        List<ManagedLedgerInternalStats.LedgerInfo> ledgers = admin.topics().getInternalStats(topicName).ledgers;
        assertEquals(ledgers.size(), 1);
        assertNotEquals(ledgers.get(0).ledgerId, msgIdInDeletedLedger5.getLedgerId());

        // Verify backlog and markDeletePosition is correct.
        Awaitility.await().untilAsserted(() -> {
            SubscriptionStats subscriptionStats = admin.topics().getStats(topicName, true, true, true)
                    .getSubscriptions().get(nonDurableCursor);
            log.info("backlog size: {}", subscriptionStats.getMsgBacklog());
            assertEquals(subscriptionStats.getMsgBacklog(), 0);
            ManagedLedgerInternalStats.CursorStats cursorStats =
                    admin.topics().getInternalStats(topicName).cursors.get(nonDurableCursor);
            String[] ledgerIdAndEntryId = cursorStats.markDeletePosition.split(":");
            PositionImpl actMarkDeletedPos =
                    PositionImpl.get(Long.valueOf(ledgerIdAndEntryId[0]), Long.valueOf(ledgerIdAndEntryId[1]));
            PositionImpl expectedMarkDeletedPos =
                    PositionImpl.get(msgIdInDeletedLedger5.getLedgerId(), msgIdInDeletedLedger5.getEntryId());
            log.info("Expected mark deleted position: {}", expectedMarkDeletedPos);
            log.info("Actual mark deleted position: {}", cursorStats.markDeletePosition);
            Assert.assertTrue(actMarkDeletedPos.compareTo(expectedMarkDeletedPos) >= 0);
        });

        // Clear the incoming queue of the reader for next test.
        while (true) {
            Message<String> msg = reader.readNext(2, TimeUnit.SECONDS);
            if (msg == null) {
                break;
            }
            log.info("clear msg: {}", msg.getValue());
        }

        // The following tests are designed to verify the api "getNumberOfEntries" and "consumedEntries" still work
        // after changes.See the code-description added with the PR https://github.com/apache/pulsar/pull/10667.
        PersistentTopic persistentTopic =
                (PersistentTopic) pulsar.getBrokerService().getTopic(topicName, false).join().get();
        ManagedLedgerImpl ml = (ManagedLedgerImpl) persistentTopic.getManagedLedger();
        ManagedCursorImpl cursor = (ManagedCursorImpl) ml.getCursors().get(nonDurableCursor);

        // Verify "getNumberOfEntries" if there is no entries to consume.
        assertEquals(0, cursor.getNumberOfEntries());
        assertEquals(0, ml.getNumberOfEntries());

        // Verify "getNumberOfEntries" if there is 1 entry to consume.
        producer.send("6");
        producer.send("7");
        Awaitility.await().untilAsserted(() -> {
            assertEquals(2, ml.getNumberOfEntries());
            // Since there is one message has been pulled into the incoming queue of reader. There is only one messages
            // waiting to cursor read.
            assertEquals(1, cursor.getNumberOfEntries());
        });

        // Verify "consumedEntries" is correct.
        ManagedLedgerInternalStats.CursorStats cursorStats =
                admin.topics().getInternalStats(topicName).cursors.get(nonDurableCursor);
        // "messagesConsumedCounter" should be 0 after unload the topic.
        // Note: "topic_internal_stat.cursor.messagesConsumedCounter" means how many messages were acked on this
        //   cursor. The similar one "topic_stats.lastConsumedTimestamp" means the last time of sending messages to
        //   the consumer.
        assertEquals(0, cursorStats.messagesConsumedCounter);
        Message<String> msg6 = reader.readNext(2, TimeUnit.SECONDS);
        assertEquals(msg6.getValue(), "6");
        Message<String> msg7 = reader.readNext(2, TimeUnit.SECONDS);
        assertEquals(msg7.getValue(), "7");
        Awaitility.await().untilAsserted(() -> {
            // "messagesConsumedCounter" should be 2 after consumed 2 message.
            ManagedLedgerInternalStats.CursorStats cStat =
                    admin.topics().getInternalStats(topicName).cursors.get(nonDurableCursor);
            assertEquals(2, cStat.messagesConsumedCounter);
        });

        // cleanup.
        reader.close();
        producer.close();
        admin.topics().delete(topicName, false);
    }
}
