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

import java.io.File;
import java.io.IOException;
import java.util.*;

import static org.apache.bookkeeper.bookie.BookKeeperServerStats.BOOKIE_SCOPE;

@RunWith(Parameterized.class)
public class InterleavedLedgerStorageTest {
    private Optional<RateLimiter> rateLimiter;
    private boolean shouldThrowException;
    private int expectedInconsistenciesCount;

    private TestStatsProvider statsProvider;
    private ServerConfiguration serverConfig;
    private LedgerDirsManager ledgerDirsManager;
    private InterleavedLedgerStorage ledgerStorage;

    private static final long ENTRIES_PER_WRITE = 1;
    private static final long TOTAL_WRITES = 40;
    private static final long TOTAL_LEDGERS = 3;

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {Option.EMPTY},
                {Option.VALID},
                {Option.INVALID},
        });
    }

    public enum Option {
        EMPTY, VALID, INVALID
    }

    public InterleavedLedgerStorageTest(Option option) throws IOException {
        initialize(option);
    }

    private void initialize(Option option) throws IOException {
        setupTestStorage();

        shouldThrowException = false;
        switch (option) {
            case INVALID:
                rateLimiter = null;  // Scenario con rate limiter non valido
                shouldThrowException = true;
                break;
            case EMPTY:
                rateLimiter = Optional.empty();  // Scenario con rate limiter valido, ma vuoto
                expectedInconsistenciesCount = 0;
                break;
            case VALID:
                rateLimiter = Optional.of(RateLimiter.create(1));  // Scenario con rate limiter valido
                expectedInconsistenciesCount = 0;
                break;
        }
    }

    private void setupTestStorage() throws IOException {
        this.statsProvider = new TestStatsProvider();
        this.serverConfig = TestBKConfiguration.newServerConfiguration();

        ledgerStorage = new InterleavedLedgerStorage();
        CheckpointSource checkpointSource = createCheckpointSource();
        Checkpointer checkpointer = createCheckpointer();

        File tempDir = createTempDirectory();
        serverConfig.setLedgerDirNames(new String[]{tempDir.toString()});
        ledgerDirsManager = new LedgerDirsManager(serverConfig, serverConfig.getLedgerDirs(),
                new DiskChecker(serverConfig.getDiskUsageThreshold(), serverConfig.getDiskUsageWarnThreshold()));

        DefaultEntryLogger entryLogger = new DefaultEntryLogger(TestBKConfiguration.newServerConfiguration());
        ledgerStorage.initializeWithEntryLogger(
                serverConfig, null, ledgerDirsManager, ledgerDirsManager, entryLogger,
                statsProvider.getStatsLogger(BOOKIE_SCOPE));

        ledgerStorage.setCheckpointer(checkpointer);
        ledgerStorage.setCheckpointSource(checkpointSource);

        populateLedgerStorageWithEntries();
    }

    private CheckpointSource createCheckpointSource() {
        return new CheckpointSource() {
            @Override
            public Checkpoint newCheckpoint() {
                return Checkpoint.MAX;
            }

            @Override
            public void checkpointComplete(Checkpoint checkpoint, boolean compact) {
            }
        };
    }

    private Checkpointer createCheckpointer() {
        return new Checkpointer() {
            @Override
            public void startCheckpoint(CheckpointSource.Checkpoint checkpoint) {
            }

            @Override
            public void start() {
            }
        };
    }

    private File createTempDirectory() throws IOException {
        File tempDir = File.createTempFile("BookkeeperTempTest", ".dir");
        tempDir.delete();
        tempDir.mkdir();
        File currentDir = BookieImpl.getCurrentDirectory(tempDir);
        BookieImpl.checkDirectoryStructure(currentDir);
        return tempDir;
    }

    private void populateLedgerStorageWithEntries() throws IOException {
        for (long ledgerId = 0; ledgerId < TOTAL_LEDGERS; ledgerId++) {
            ledgerStorage.setMasterKey(ledgerId, ("ledger-" + ledgerId).getBytes());
            ledgerStorage.setFenced(ledgerId);
            for (long entryId = 0; entryId < TOTAL_WRITES; entryId++) {
                ByteBuf entry = Unpooled.buffer(128);
                entry.writeLong(ledgerId);
                entry.writeLong(entryId * ENTRIES_PER_WRITE);
                entry.writeBytes(("entry-" + entryId).getBytes());
                ledgerStorage.addEntry(entry);
            }
        }
    }

    @Test
    public void testLocalConsistencyCheck() {
        try {
            List<LedgerStorage.DetectedInconsistency> detectedInconsistencies = ledgerStorage.localConsistencyCheck(rateLimiter);

            // Verifica che il numero di inconsistenze corrisponda al valore atteso
            Assert.assertEquals(expectedInconsistenciesCount, detectedInconsistencies.size());
            Assert.assertFalse(shouldThrowException);

        } catch (Exception e) {
            // Verifica che venga lanciata l'eccezione come previsto nel caso di configurazione invalida
            Assert.assertTrue(shouldThrowException);
        }
    }
}
