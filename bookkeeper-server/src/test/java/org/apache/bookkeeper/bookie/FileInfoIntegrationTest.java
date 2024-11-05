package org.apache.bookkeeper.bookie;

import org.junit.jupiter.api.*;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.file.Files;

import static org.junit.jupiter.api.Assertions.*;

public class FileInfoIntegrationTest {

    private File originalFile;
    private File targetFile;
    private FileInfo fileInfo;
    private static final byte[] MASTER_KEY = "testMasterKey".getBytes();

    @BeforeEach
    public void setUp() throws IOException {
        // Crea file temporanei per il test
        originalFile = File.createTempFile("testFile", ".idx");
        targetFile = new File(originalFile.getParentFile(), "movedTestFile.idx");

        // Inizializza l'oggetto FileInfo
        fileInfo = new FileInfo(originalFile, MASTER_KEY, FileInfo.CURRENT_HEADER_VERSION);

        // Scrive alcuni dati nel file per verificarne la copia
        ByteBuffer buffer = ByteBuffer.wrap("TestData".getBytes());
        fileInfo.write(new ByteBuffer[]{buffer}, 0);
        fileInfo.flushHeader(); // Flush per assicurarsi che i dati siano scritti
    }

    @Test
    public void testMoveToNewLocation() throws IOException {
        // Verifica che il file di origine esista
        assertTrue(originalFile.exists(), "Il file originale dovrebbe esistere");

        // Esegue il metodo moveToNewLocation
        fileInfo.moveToNewLocation(targetFile, Long.MAX_VALUE);

        // Verifica che il file di origine sia stato eliminato
        assertFalse(originalFile.exists(), "Il file originale dovrebbe essere eliminato");

        // Verifica che il file di destinazione esista
        assertTrue(targetFile.exists(), "Il file di destinazione dovrebbe esistere");

        // Verifica che i dati siano stati copiati correttamente
        byte[] targetData = Files.readAllBytes(targetFile.toPath());
        String content = new String(targetData).trim();
        assertTrue(content.contains("TestData"), "Il contenuto del file di destinazione dovrebbe includere 'TestData'");

        // Verifica che il nuovo file sia stato associato a fileInfo
        assertEquals(targetFile, fileInfo.getLf(), "Il file info dovrebbe ora puntare al file di destinazione");
    }

    @AfterEach
    public void tearDown() throws IOException {
        // Pulisce i file temporanei
        if (originalFile.exists()) {
            originalFile.delete();
        }
        if (targetFile.exists()) {
            targetFile.delete();
        }
    }
}
