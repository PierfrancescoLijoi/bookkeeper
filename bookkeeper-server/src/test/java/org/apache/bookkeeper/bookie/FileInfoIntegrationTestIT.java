package org.apache.bookkeeper.bookie;

import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.util.SnapshotMap;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;


class FileInfoIntegrationTestIT {

    private File originalFile;
    private FileInfo fileInfoMock;
    private IndexPersistenceMgr indexPersistenceMgr;

    @BeforeEach
    void setUp() throws IOException {
        // Crea file temporaneo per il file originale
        originalFile = File.createTempFile("testFile", ".idx");

        // Mock LedgerDirsManager per passarlo a IndexPersistenceMgr
        LedgerDirsManager ledgerDirsManager = Mockito.mock(LedgerDirsManager.class);
        Mockito.when(ledgerDirsManager.pickRandomWritableDirForNewIndexFile(null))
                .thenReturn(originalFile.getParentFile());

        // Inizializza IndexPersistenceMgr
        indexPersistenceMgr = new IndexPersistenceMgr(
                1024, // pageSize
                64,   // entriesPerPage
                new ServerConfiguration(),
                new SnapshotMap<>(),
                ledgerDirsManager,
                NullStatsLogger.INSTANCE
        );

        // Crea un mock di FileInfo e configura un'eccezione per moveToNewLocation
        fileInfoMock = mock(FileInfo.class);
        when(fileInfoMock.getLf()).thenReturn(originalFile);
        doThrow(new IOException("Simulated IO Exception")).when(fileInfoMock).moveToNewLocation(any(File.class), anyLong());

        System.out.println("Percorso file originale: " + originalFile.getAbsolutePath());
    }

    @Test
    void testMoveLedgerIndexFileThrowsException() throws NoSuchMethodException {
        // Accedi al metodo privato moveLedgerIndexFile tramite riflessione
        Method moveLedgerIndexFileMethod = IndexPersistenceMgr.class.getDeclaredMethod("moveLedgerIndexFile", Long.class, FileInfo.class);
        moveLedgerIndexFileMethod.setAccessible(true);

        // Verifica che venga lanciata un'eccezione durante il trasferimento
        InvocationTargetException exception = assertThrows(InvocationTargetException.class, () -> {
            moveLedgerIndexFileMethod.invoke(indexPersistenceMgr, 1L, fileInfoMock);
        });

        // Verifica che la causa dell'eccezione sia un'IOException e che il messaggio sia quello atteso
        Throwable cause = exception.getCause();
        assertTrue(cause instanceof IOException, "La causa dovrebbe essere un'IOException");
        assertEquals("Simulated IO Exception", cause.getMessage(), "Il messaggio dell'IOException dovrebbe corrispondere");
    }

    @AfterEach
    void tearDown() {
        // Elimina il file temporaneo
        if (originalFile.exists()) {
            originalFile.delete();
        }
    }
}
