package org.apache.bookkeeper.bookie;

import org.apache.bookkeeper.util.IOUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)
public class FileInfoTest {

    private FileInfo originalFileInfo;
    private File destinationFile;
    private boolean isExceptionExpected;
    private String testContent;
    private long destinationSize;
    private int expectedReadSize;

    enum ParamFile {
        NULL, INVALID, VALID
    }

    enum ParamSize {
        NEG, ZERO, CURRENT_PLUS_ONE, CURRENT, CURRENT_MINUS_ONE
    }

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
                // Casi di test per file NULL
                {ParamFile.NULL, ParamSize.NEG},            // Riga 1: File NULL, size -1
                {ParamFile.NULL, ParamSize.ZERO},           // Riga 2: File NULL, size 0
                {ParamFile.NULL, ParamSize.CURRENT_MINUS_ONE}, // Riga 3: File NULL, size attuale - 1
                {ParamFile.NULL, ParamSize.CURRENT},        // Riga 4: File NULL, size attuale
                {ParamFile.NULL, ParamSize.CURRENT_PLUS_ONE}, // Riga 5: File NULL, size attuale + 1

                // Casi di test per file VALID
//                {ParamFile.VALID, ParamSize.NEG},           // Riga 6: File valido, size -1, eccezione attesa
                {ParamFile.VALID, ParamSize.ZERO},          // Riga 7: File valido, size 0
                {ParamFile.VALID, ParamSize.CURRENT_MINUS_ONE}, // Riga 8: File valido, size attuale - 1
                {ParamFile.VALID, ParamSize.CURRENT_PLUS_ONE}, // Riga 10: File valido, size attuale + 1
                {ParamFile.VALID, ParamSize.CURRENT},       // Riga 9: File valido, size attuale

                // Casi di test per file INVALID
//                {ParamFile.INVALID, ParamSize.NEG},         // Riga 11: File non valido, size -1
//                {ParamFile.INVALID, ParamSize.ZERO},        // Riga 12: File non valido, size 0
//                {ParamFile.INVALID, ParamSize.CURRENT_MINUS_ONE}, // Riga 13: File non valido, size attuale - 1
//                {ParamFile.INVALID, ParamSize.CURRENT},     // Riga 14: File non valido, size attuale
//                {ParamFile.INVALID, ParamSize.CURRENT_PLUS_ONE} // Riga 15: File non valido, size attuale + 1
        });
    }

    // Metodo per configurare i parametri di test
    void setupTestParameters(ParamFile fileType, ParamSize sizeType) throws IOException {
        File tempFile = createTemporaryFile("testFileInfo");
        this.testContent = "ciao, come va?";  // Contenuto di test
        String masterkey = "";
        this.originalFileInfo = new FileInfo(tempFile, masterkey.getBytes(), 0);

        ByteBuffer bb[] = new ByteBuffer[1];
        bb[0] = ByteBuffer.wrap(this.testContent.getBytes());
        this.originalFileInfo.write(bb, 0);

        this.isExceptionExpected = false;

        // Configura il tipo di file (NULL, INVALID, VALID)
        switch (fileType) {
            case NULL:
                this.destinationFile = null;
                this.isExceptionExpected = true; // Eccezione attesa per file NULL
                break;
            case VALID:
                this.destinationFile = createTemporaryFile("testFileInfoNewFile");
                // Eccezione attesa per FileValido con dimensione negativa (Riga 6)
                if (sizeType == ParamSize.NEG) {
                    this.isExceptionExpected = true;
                }
                break;
            case INVALID:
                this.destinationFile = createTemporaryFile("testFileInfoNewFile");
                this.destinationFile.delete();  // Simula un file non valido
                this.isExceptionExpected = true; // Eccezione attesa per file non valido
                break;
        }

        // Configura il tipo di dimensione (NEG, ZERO, CURRENT_PLUS_ONE, CURRENT, CURRENT_MINUS_ONE)
        switch (sizeType) {
            case NEG:
                this.destinationSize = -1;        // Size negativa (casi nelle righe 1, 6, 11)
                this.expectedReadSize = 0;        // Nessun byte atteso per copia
                break;
            case ZERO:
                this.destinationSize = 0;         // Size zero (casi nelle righe 2, 7, 12)
                this.expectedReadSize = 0;        // Nessun byte atteso per copia
                break;
            case CURRENT_MINUS_ONE:
                this.expectedReadSize = this.testContent.length() - 1; // Copia incompleta (righe 3, 8, 13)
                this.destinationSize = 1024 + this.testContent.length() - 1;
                break;
            case CURRENT:
                this.expectedReadSize = this.testContent.length();      // Copia completa (righe 4, 9, 14)
                this.destinationSize = 1024 + this.testContent.length();
                break;
            case CURRENT_PLUS_ONE:
                this.expectedReadSize = this.testContent.length();      // Copia completa con size maggiore (righe 5, 10, 15)
                this.destinationSize = 1024 + this.testContent.length() + 1;
                break;
        }
    }

    // Costruttore che configura i parametri di test in base ai valori
    public FileInfoTest(ParamFile fileType, ParamSize sizeType) {
        try {
            setupTestParameters(fileType, sizeType);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    // Metodo helper per creare file temporanei
    private File createTemporaryFile(String suffix) throws IOException {
        return IOUtils.createTempFileAndDeleteOnExit("bookie", suffix);
    }

    // Test principale per verificare lo spostamento del file in una nuova posizione
    @Test
    public void testMoveToNewLocation() {
        try {
            this.originalFileInfo.moveToNewLocation(this.destinationFile, this.destinationSize);
            ByteBuffer buffer = ByteBuffer.allocate(this.expectedReadSize + 10);
            FileInfo newFileInfo = new FileInfo(this.originalFileInfo.getLf(), "".getBytes(), 0);
            int bytesRead = newFileInfo.read(buffer, 0, true);

            // Verifica del caso NULL
            if (this.destinationFile == null) { // Righe 1-5
                Assert.assertEquals("Mi aspetto che non ci siano byte letti da un file nullo.", 0, bytesRead);
                Assert.assertTrue("Mi aspetto un'eccezione per file nullo.", this.isExceptionExpected);
            }
            // Verifica del caso INVALID
            else if (!this.destinationFile.exists()) { // Righe 11-15
                Assert.assertEquals("Mi aspetto che non ci siano byte letti da un file non valido.", 0, bytesRead);
                Assert.assertTrue("Mi aspetto un'eccezione per file non valido.", this.isExceptionExpected);
            }
            // Verifica dei file VALID
            else { // Righe 6-10
                if (this.expectedReadSize > 0) {
                    for (int i = 0; i < this.expectedReadSize; i++) {
                        Assert.assertEquals("Mismatch di carattere atteso a indice " + i,
                                testContent.charAt(i), (char) buffer.array()[i]);
                    }
                }
                Assert.assertFalse("Non mi aspettavo un'eccezione per file valido", this.isExceptionExpected);
            }
        } catch (Exception e) {
            Assert.assertTrue("Mi aspettavo un'eccezione: " + e.getMessage(), this.isExceptionExpected);
        }
    }

}
