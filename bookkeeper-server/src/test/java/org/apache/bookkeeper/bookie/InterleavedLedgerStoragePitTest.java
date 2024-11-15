package org.apache.bookkeeper.bookie;

import com.google.common.util.concurrent.RateLimiter;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import org.apache.bookkeeper.bookie.confUtils.TestBKConfiguration;
import org.apache.bookkeeper.bookie.confUtils.TestStatsProvider;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.util.DiskChecker;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.util.*;

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.*;


@RunWith(Parameterized.class)
public class InterleavedLedgerStoragePitTest {

    private Optional<RateLimiter> rateLimiter;
    private boolean exceptionExpected;
    private long expectedInconsistencies;

    private TestStatsProvider statsProvider = new TestStatsProvider();
    private ServerConfiguration config = TestBKConfiguration.newServerConfiguration();
    private LedgerDirsManager ledgerDirsManager;
    private InterleavedLedgerStorage ledgerStorage;

    private static final long NUM_WRITES = 10;
    private static final long ENTRIES_PER_WRITE = 2;
    private static final long NUM_LEDGERS = 2;

    @Parameterized.Parameters
    public static Collection<Object[]> parameterData() {
        return Arrays.asList(new Object[][] {

                { ParamOption.INVALID, ConfigOption.MOCK_NO_LEDGER_EXCEPTION },

                { ParamOption.VALID, ConfigOption.MOCK_NO_LEDGER_EXCEPTION },

                { ParamOption.EMPTY, ConfigOption.MOCK_NO_LEDGER_EXCEPTION }
        });
    }

    public enum ParamOption {
        EMPTY, VALID, INVALID
    }

    public enum ConfigOption {
        MOCK_NO_LEDGER_EXCEPTION
    }

    private ParamOption paramOption;
    private ConfigOption configOption;

    public InterleavedLedgerStoragePitTest(ParamOption paramOption, ConfigOption configOption) {
        this.paramOption = paramOption;
        this.configOption = configOption;
        setupTestScenario(paramOption, configOption);
    }

    private void setupTestScenario(ParamOption paramOption, ConfigOption configOption) {
        exceptionExpected = false;
        expectedInconsistencies = 0;

        switch (configOption) {
            case MOCK_NO_LEDGER_EXCEPTION:
                try {
                    initializeStorageWithEntries();
                    mockEntryLoggerToThrowException(Bookie.NoLedgerException.class);
                    expectedInconsistencies = ENTRIES_PER_WRITE;
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                break;

        }

        configureRateLimiterAndExceptionHandling(paramOption);
    }

    private void mockEntryLoggerToThrowException(Class<? extends Exception> exceptionClass) throws IOException, DefaultEntryLogger.EntryLookupException {
        ledgerStorage.entryLogger = mock(DefaultEntryLogger.class);
        Mockito.doThrow(exceptionClass).when(ledgerStorage.entryLogger).checkEntry(anyLong(), anyLong(), anyLong());
    }

    private void configureRateLimiterAndExceptionHandling(ParamOption paramOption) {
        switch (paramOption) {
            case INVALID:
                this.rateLimiter = null;
                exceptionExpected = true;
                break;
            case EMPTY:
                this.rateLimiter = Optional.empty();
                exceptionExpected = false;
                break;
            case VALID:
                this.rateLimiter = Optional.of(RateLimiter.create(1));
                exceptionExpected = false;
                break;
        }
    }

    private void initializeStorageWithEntries() throws IOException {
        ledgerStorage = new InterleavedLedgerStorage();
        prepareStorageEnvironment();
        populateEntries();
    }

    private void prepareStorageEnvironment() throws IOException {
        File tempDir = setupStorageDirectories();
        ledgerDirsManager = setupLedgerDirsManager(tempDir);
        ledgerStorage.initializeWithEntryLogger(config, null, ledgerDirsManager, ledgerDirsManager,
                new DefaultEntryLogger(config), statsProvider.getStatsLogger("BOOKIE_SCOPE"));
        ledgerStorage.setCheckpointer(new NoOpCheckpointer());
        ledgerStorage.setCheckpointSource(new NoOpCheckpointSource());
    }

    private File setupStorageDirectories() throws IOException {
        File tempDir = File.createTempFile("bokkeeperPitTest", ".dir");
        tempDir.delete();
        tempDir.mkdir();
        File currentDir = BookieImpl.getCurrentDirectory(tempDir);
        BookieImpl.checkDirectoryStructure(currentDir);
        config.setLedgerDirNames(new String[] { tempDir.toString() });
        return tempDir;
    }

    private LedgerDirsManager setupLedgerDirsManager(File tempDir) throws IOException {
        return new LedgerDirsManager(config, config.getLedgerDirs(),
                new DiskChecker(config.getDiskUsageThreshold(), config.getDiskUsageWarnThreshold()));
    }

    private void populateEntries() throws IOException {
        for (long entryId = 0; entryId < NUM_WRITES; entryId++) {
            for (long ledgerId = 0; ledgerId < NUM_LEDGERS; ledgerId++) {
                initializeLedgerIfNeeded(ledgerId, entryId);
                ByteBuf entry = createEntryBuffer(ledgerId, entryId);
                ledgerStorage.addEntry(entry);
            }
        }
    }

    private void initializeLedgerIfNeeded(long ledgerId, long entryId) throws IOException {
        if (entryId == 0) {
            ledgerStorage.setMasterKey(ledgerId, ("ledger-" + ledgerId).getBytes());
            ledgerStorage.setFenced(ledgerId);
        }
    }

    private ByteBuf createEntryBuffer(long ledgerId, long entryId) {
        ByteBuf entry = Unpooled.buffer(128);
        entry.writeLong(ledgerId);
        entry.writeLong(entryId * ENTRIES_PER_WRITE);
        entry.writeBytes(("entry-" + entryId).getBytes());
        return entry;
    }

    @Test
    public void pitKillingMutation() {
        try {
            List<LedgerStorage.DetectedInconsistency> inconsistencies = ledgerStorage.localConsistencyCheck(rateLimiter);

            System.out.println("Inconsistencies found: " + inconsistencies.size());

            Assert.assertEquals("Inconsistencies count mismatch", expectedInconsistencies, inconsistencies.size());

            if (exceptionExpected) {
                Assert.fail("Expected exception but none was thrown during the consistency check");
            }
        } catch (IOException e) {
            handleExceptionDuringTest(e);
        } catch (Exception e) {
            handleUnexpectedException(e);
        }
    }

    private void handleExceptionDuringTest(IOException e) {
        e.printStackTrace();
        if (exceptionExpected) {
            Assert.assertTrue("Expected exception was not thrown", exceptionExpected);
        } else {
            Assert.fail("Unexpected exception: " + e.getMessage());
        }
    }

    private void handleUnexpectedException(Exception e) {
        e.printStackTrace();
        Assert.fail("Unexpected exception during localConsistencyCheck: " + e.getMessage());
    }

    // Implementazioni no-op per semplificare il codice ai fini dei test
    private static class NoOpCheckpointer implements Checkpointer {
        @Override
        public void startCheckpoint(CheckpointSource.Checkpoint checkpoint) {}
        @Override
        public void start() {}
    }

    private static class NoOpCheckpointSource implements CheckpointSource {
        @Override
        public Checkpoint newCheckpoint() {
            return Checkpoint.MAX;
        }
        @Override
        public void checkpointComplete(Checkpoint checkpoint, boolean compact) {}
    }
}

