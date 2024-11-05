package org.apache.bookkeeper.bookie;

import org.apache.bookkeeper.util.IOUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

public class FileInfoPitTest {

    private FileInfo originalFileInfo;

    // Metodo helper per creare file temporanei
    private File createTemporaryFile(String suffix) throws IOException {
        return IOUtils.createTempFileAndDeleteOnExit("bookie", suffix);
    }

    // Test per uccidere la mutazione nel costrutto `if (size > fc.size())`
    @Test
    public void testMoveToNewLocationBoundary() throws Exception {
        // Simuliamo un file valido
        File validFile = createTemporaryFile("validFile");
        long fileSize = 1024; // Dimensione del file valida
        String masterkey = "";
        this.originalFileInfo = new FileInfo(validFile, masterkey.getBytes(), 0);

        // Scriviamo alcuni dati nel file
        ByteBuffer bufferToWrite = ByteBuffer.allocate((int) fileSize);
        for (int i = 0; i < fileSize; i++) {
            bufferToWrite.put((byte) 'a'); // Scriviamo caratteri 'a' nel buffer
        }
        bufferToWrite.flip(); // Ripristina la posizione per la lettura
        this.originalFileInfo.write(new ByteBuffer[]{bufferToWrite}, 0);

        // Casi di test per la dimensione
        long[] testSizes = {fileSize - 1, fileSize, fileSize + 1};

        for (long size : testSizes) {
            try {
                this.originalFileInfo.moveToNewLocation(validFile, size);
                // Creiamo un nuovo FileInfo per la verifica
                FileInfo newFileInfo = new FileInfo(validFile, "".getBytes(), 0);
                ByteBuffer buffer = ByteBuffer.allocate((int) size);
                int bytesRead = newFileInfo.read(buffer, 0, true);

                // Verifichiamo che la lettura avvenga in base alla dimensione specificata
                if (size < fileSize) {
                    Assert.assertEquals("Ci si aspetta che la lettura sia limitata.", size, bytesRead);
                } else {
                    Assert.assertEquals("Ci si aspetta che la lettura sia completa.", fileSize, bytesRead);
                }
            } catch (Exception e) {
                Assert.assertTrue("Mi aspettavo un'eccezione per dimensione " + size + ": " + e.getMessage(), size > fileSize);
            }
        }
    }
}
