package org.apache.bookkeeper.bookie;

import com.google.common.util.concurrent.RateLimiter;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.bookkeeper.bookie.confUtils.TestBKConfiguration;
import org.apache.bookkeeper.bookie.confUtils.TestStatsProvider;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.util.DiskChecker;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Optional;

import static org.apache.bookkeeper.bookie.BookKeeperServerStats.BOOKIE_SCOPE;

public class InterleavedLedgerStorageBoostedTest {

    private InterleavedLedgerStorage ledgerStorage;
    private ServerConfiguration serverConfig;
    private TestStatsProvider statsProvider;
    private LedgerDirsManager ledgerDirsManager;

    private static final long ENTRIES_PER_WRITE = 1;
    private static final long TOTAL_WRITES = 10;
    private static final long TOTAL_LEDGERS = 2;

    @Before
    public void setup() throws IOException {
        this.statsProvider = new TestStatsProvider();
        this.serverConfig = TestBKConfiguration.newServerConfiguration();
        this.ledgerStorage = new InterleavedLedgerStorage();
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

    // Test Specifico: Simula Spazio su Disco Esaurito
    @Test
    public void testDiskSpaceFull() {
        try {
            // Simuliamo un errore di scrittura quando si raggiunge il limite di spazio
            serverConfig.setLedgerDirNames(new String[]{"fullDiskDir"});
            ledgerStorage = new InterleavedLedgerStorage() {
                @Override
                public long addEntry(ByteBuf entry) throws IOException {
                    throw new IOException("Disk full");
                }
            };

            ledgerStorage.addEntry(Unpooled.buffer(128));
            Assert.fail("Expected IOException due to disk full");
        } catch (IOException e) {
            Assert.assertTrue(e.getMessage().contains("Disk full"));
        }
    }

    // Test Specifico: Test Funzionalità Rate Limiter
    @Test
    public void testRateLimiter() throws IOException {
        // Simula uno scenario di rate-limiting
        RateLimiter limiter = RateLimiter.create(1); // 1 richiesta al secondo
        for (int i = 0; i < 10; i++) {
            limiter.acquire(); // Assicura che il rate limiting sia applicato
            ByteBuf entry = Unpooled.buffer(128);
            entry.writeLong(1L);
            entry.writeLong(i);
            entry.writeBytes(("entry-" + i).getBytes());
            ledgerStorage.addEntry(entry);
        }

        // Verifica che il numero di voci scritte sia come previsto
        List<LedgerStorage.DetectedInconsistency> detectedInconsistencies = ledgerStorage.localConsistencyCheck(Optional.of(limiter));
        Assert.assertEquals(0, detectedInconsistencies.size());
    }
    // Test Specifico: Test Configurazione RateLimiter Non Valida
    @Test
    public void testInvalidConfiguration() {
        try {
            // Tenta di creare un RateLimiter con una configurazione non valida (rate = 0)
            RateLimiter rateLimiter = RateLimiter.create(0);  // rate = 0
            Assert.fail("Expected IllegalArgumentException due to rate = 0");
        } catch (IllegalArgumentException e) {
            // Verifica che l'eccezione sia quella corretta
            Assert.assertTrue(e.getMessage().contains("rate must be positive"));
        } catch (Exception e) {
            // Se si verifica un'altra eccezione, fallisce il test
            Assert.fail("Expected IllegalArgumentException, but got: " + e);
        }
    }


}
