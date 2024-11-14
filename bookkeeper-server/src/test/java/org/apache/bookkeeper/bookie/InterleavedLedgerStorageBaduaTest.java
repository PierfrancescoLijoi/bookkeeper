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
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

import static org.apache.bookkeeper.bookie.BookKeeperServerStats.BOOKIE_SCOPE;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;

@RunWith(Parameterized.class)
public class InterleavedLedgerStorageBaduaTest {

    private Optional<RateLimiter> rateLimiterOption;
    private boolean shouldThrowException;
    private int expectedInconsistencies;

    private TestStatsProvider statsProvider = new TestStatsProvider();
    private ServerConfiguration config = TestBKConfiguration.newServerConfiguration();
    private LedgerDirsManager dirsManager;
    private InterleavedLedgerStorage storage;

    private static final long WRITE_COUNT = 10;
    private static final long ENTRIES_PER_WRITE = 2;
    private static final long LEDGER_COUNT = 2;

    @Parameterized.Parameters
    public static Collection<Object[]> testData() {
        // Definisco i parametri di test da utilizzare
        return Arrays.asList(new Object[][] {

                {ParamType.EMPTY, ConfigType.PRESENT},
                {ParamType.EMPTY, ConfigType.MOCK},
                {ParamType.EMPTY, ConfigType.EMPTY},

                {ParamType.VALID, ConfigType.PRESENT},
                {ParamType.VALID, ConfigType.MOCK},
                {ParamType.VALID, ConfigType.EMPTY},

        });
    }

    public enum ConfigType {
        EMPTY, PRESENT, MOCK
    }

    public enum ParamType {
        EMPTY, VALID
    }

    public InterleavedLedgerStorageBaduaTest(ParamType paramOption, ConfigType configOption) throws DefaultEntryLogger.EntryLookupException {
        // Configuro il test in base ai parametri forniti
        configure(paramOption, configOption);
    }

    private void configure(ParamType paramOption, ConfigType configOption) throws DefaultEntryLogger.EntryLookupException {
        // Imposto le variabili in base ai parametri forniti
        shouldThrowException = false;
        expectedInconsistencies = 0;
        rateLimiterOption = determineRateLimiter(paramOption);

        switch (configOption) {
            case EMPTY:
                prepareEmptyStorage();
                break;
            case PRESENT:
                prepareStorageWithData();
                break;
            case MOCK:
                prepareStorageWithMock();
                break;
        }
    }

    private Optional<RateLimiter> determineRateLimiter(ParamType paramOption) {
        // Determino se devo usare il RateLimiter in base ai parametri
        switch (paramOption) {
            case EMPTY:
                return Optional.empty();
            case VALID:
                return Optional.of(RateLimiter.create(1));
            default:
                return Optional.empty();
        }
    }


    private void prepareEmptyStorage() {
        // Preparo uno storage vuoto
        try {
            storage = initializeStorage();
        } catch (IOException e) {
            throw new RuntimeException("Impossibile inizializzare uno storage vuoto", e);
        }
    }

    private void prepareStorageWithData() {
        // Preparo uno storage con dati di test
        try {
            storage = initializeStorage();
            addTestEntriesToStorage();
        } catch (IOException e) {
            throw new RuntimeException("Impossibile inizializzare lo storage con dati", e);
        }
    }

    private void prepareStorageWithMock() throws DefaultEntryLogger.EntryLookupException {
        // Preparo uno storage con un mock per testare il comportamento in caso di eccezioni
        try {
            storage = initializeStorage();
            addTestEntriesToStorage();
            mockEntryLoggerToThrowException();
            expectedInconsistencies = 20;
        } catch (IOException e) {
            throw new RuntimeException("Impossibile inizializzare lo storage con mock", e);
        }
    }

    private InterleavedLedgerStorage initializeStorage() throws IOException {
        // Inizializzo una nuova istanza di InterleavedLedgerStorage
        InterleavedLedgerStorage storageInstance = new InterleavedLedgerStorage();

        CheckpointSource checkpointSource = createCheckpointSource();
        Checkpointer checkpointer = createCheckpointer();

        File tempDir = createTempDirectory();
        dirsManager = initializeLedgerDirsManager(tempDir);

        DefaultEntryLogger entryLogger = new DefaultEntryLogger(config);
        storageInstance.initializeWithEntryLogger(config, null, dirsManager, dirsManager, entryLogger,
                statsProvider.getStatsLogger(BOOKIE_SCOPE));

        storageInstance.setCheckpointer(checkpointer);
        storageInstance.setCheckpointSource(checkpointSource);

        return storageInstance;
    }

    private CheckpointSource createCheckpointSource() {
        // Creo una nuova sorgente di checkpoint
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
        // Creo un nuovo checkpointer (non fa nulla per ora)
        return new Checkpointer() {
            @Override
            public void startCheckpoint(CheckpointSource.Checkpoint checkpoint) {
            }

            @Override
            public void start() {
                // nessuna operazione
            }
        };
    }

    private File createTempDirectory() throws IOException {
        // Creo una directory temporanea per i test
        File tempDir = File.createTempFile("bkTest", ".dir");
        tempDir.delete();
        tempDir.mkdir();
        return tempDir;
    }

    private LedgerDirsManager initializeLedgerDirsManager(File tempDir) throws IOException {
        // Inizializzo il gestore delle directory dei ledger
        File currentDir = BookieImpl.getCurrentDirectory(tempDir);
        BookieImpl.checkDirectoryStructure(currentDir);
        config.setLedgerDirNames(new String[]{tempDir.toString()});
        return new LedgerDirsManager(config, config.getLedgerDirs(),
                new DiskChecker(config.getDiskUsageThreshold(), config.getDiskUsageWarnThreshold()));
    }

    private void addTestEntriesToStorage() throws IOException {
        // Aggiungo voci di test allo storage
        for (long entryId = 0; entryId < WRITE_COUNT; entryId++) {
            for (long ledgerId = 0; ledgerId < LEDGER_COUNT; ledgerId++) {
                if (entryId == 0) {
                    storage.setMasterKey(ledgerId, ("ledger-" + ledgerId).getBytes());
                    storage.setFenced(ledgerId);
                }
                ByteBuf entry = Unpooled.buffer(128);
                entry.writeLong(ledgerId);
                entry.writeLong(entryId * ENTRIES_PER_WRITE);
                entry.writeBytes(("entry-" + entryId).getBytes());
                storage.addEntry(entry);
            }
        }
    }

    private void mockEntryLoggerToThrowException() throws IOException, DefaultEntryLogger.EntryLookupException {
        // Simulo un'eccezione nel logger delle voci per testare la gestione degli errori
        storage.entryLogger = mock(DefaultEntryLogger.class);
        Mockito.doThrow(DefaultEntryLogger.EntryLookupException.class)
                .when(storage.entryLogger)
                .checkEntry(anyLong(), anyLong(), anyLong());
    }


    @Test
    public void consistencyCheckTest() {
        // Eseguo il test di controllo della consistenza
        try {
            // Eseguo il controllo della consistenza e raccolgo le eventuali inconsistenze
            List<LedgerStorage.DetectedInconsistency> inconsistencies = checkConsistency();

            // Verifico che il numero di inconsistenze sia quello atteso
            Assert.assertEquals(inconsistencies.size(), expectedInconsistencies);

            // Verifico che non fosse prevista alcuna eccezione
            if (shouldThrowException) {
                Assert.fail("Era attesa un'eccezione, ma non è stata lanciata.");
            } else {
                Assert.assertFalse("Il test è fallito perché è stata lanciata un'eccezione inaspettata.", shouldThrowException);
            }

        } catch (Exception e) {
            // Se viene lanciata un'eccezione, verifico che corrisponda al comportamento atteso
            if (!shouldThrowException) {
                Assert.fail("Eccezione imprevista: " + e.getMessage());
            }
        }
    }


    private List<LedgerStorage.DetectedInconsistency> checkConsistency() throws Exception {
        // Eseguo un controllo locale della consistenza per raccogliere più dati
        List<LedgerStorage.DetectedInconsistency> inconsistencies = storage.localConsistencyCheck(rateLimiterOption);

        // Verifico che vengano rilevate nuove eccezioni (EntryLookupException)
        Assert.assertTrue(inconsistencies.size() >= expectedInconsistencies);

        return inconsistencies;
    }
}
