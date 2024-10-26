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
                {ParamFile.NULL, ParamSize.NEG},
                {ParamFile.NULL, ParamSize.ZERO},
                {ParamFile.NULL, ParamSize.CURRENT_MINUS_ONE},
                {ParamFile.NULL, ParamSize.CURRENT},
                {ParamFile.NULL, ParamSize.CURRENT_PLUS_ONE},

                // Casi di test per file INVALID
                {ParamFile.INVALID, ParamSize.NEG},
                {ParamFile.INVALID, ParamSize.ZERO},
                {ParamFile.INVALID, ParamSize.CURRENT_MINUS_ONE},
                {ParamFile.INVALID, ParamSize.CURRENT},
                {ParamFile.INVALID, ParamSize.CURRENT_PLUS_ONE},

                // Casi di test per file VALID
                {ParamFile.VALID, ParamSize.NEG},
                {ParamFile.VALID, ParamSize.ZERO},
                {ParamFile.VALID, ParamSize.CURRENT_MINUS_ONE},
                {ParamFile.VALID, ParamSize.CURRENT_PLUS_ONE},
                {ParamFile.VALID, ParamSize.CURRENT},
        });
    }

    void setupTestParameters(ParamFile fileType, ParamSize sizeType) throws IOException {
        File tempFile = createTemporaryFile("testFileInfo");
        this.testContent = "testFileInfo1";  // Contenuto di test
        String masterkey = "";
        this.originalFileInfo = new FileInfo(tempFile, masterkey.getBytes(), 0);

        ByteBuffer bb[] = new ByteBuffer[1];
        bb[0] = ByteBuffer.wrap(this.testContent.getBytes());
        this.originalFileInfo.write(bb, 0);

        this.isExceptionExpected = false;

        switch (fileType) {
            case NULL:
                this.destinationFile = null;
                this.isExceptionExpected = true; // ci aspettiamo un'eccezione
                break;
            case VALID:
                this.destinationFile = createTemporaryFile("testFileInfoNewFile");
                break;
            case INVALID:
                this.destinationFile = createTemporaryFile("testFileInfoNewFile");
                this.destinationFile.delete();  // Simula file invalido
                this.isExceptionExpected = false; // Non ci aspettiamo un'eccezione
                break;
        }

        switch (sizeType) {
            case NEG:
                this.destinationSize = -1; // Dimensione negativa
                this.expectedReadSize = 0; // Nessun byte atteso
                break;
            case ZERO:
                this.destinationSize = 0; // Size zero
                this.expectedReadSize = 0; // Nessun byte atteso
                break;
            case CURRENT_PLUS_ONE:
                this.expectedReadSize = this.testContent.length();
                this.destinationSize = 1024 + this.testContent.length() + 1;
                break;
            case CURRENT_MINUS_ONE:
                this.expectedReadSize = this.testContent.length() - 1;
                this.destinationSize = 1024 + this.testContent.length() - 1;
                break;
            case CURRENT:
                this.expectedReadSize = this.testContent.length();
                this.destinationSize = 1024 + this.testContent.length();
                break;
        }
    }

    public FileInfoTest(ParamFile fileType, ParamSize sizeType) {
        try {
            setupTestParameters(fileType, sizeType);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private File createTemporaryFile(String suffix) throws IOException {
        return IOUtils.createTempFileAndDeleteOnExit("bookie", suffix);
    }

    @Test
    public void testMoveToNewLocation() {
        try {
            this.originalFileInfo.moveToNewLocation(this.destinationFile, this.destinationSize);
            ByteBuffer buffer = ByteBuffer.allocate(this.expectedReadSize + 10);
            FileInfo newFileInfo = new FileInfo(this.originalFileInfo.getLf(), "".getBytes(), 0);
            int bytesRead = newFileInfo.read(buffer, 0, true);

            // Gestione di file NULL
            if (this.destinationFile == null) {
                Assert.assertEquals("Mi aspetto che non ci siano byte letti da un file nullo.", 0, bytesRead);
                Assert.assertTrue("Mi aspetto un'eccezione per file nullo.", this.isExceptionExpected);
            } else if (!this.destinationFile.exists()) {
                Assert.assertEquals("Mi aspetto che non ci siano byte letti da un file non valido.", 0, bytesRead);
                Assert.assertFalse("Non mi aspetto un'eccezione per file non valido.", this.isExceptionExpected);
            } else {
                // Gestione dei file validi
                if (this.expectedReadSize > 0) {
                    for (int i = 0; i < this.expectedReadSize; i++) {
                        Assert.assertEquals("Expected character mismatch at index " + i,
                                testContent.charAt(i), (char) buffer.array()[i]);
                    }
                }
                Assert.assertFalse("Non mi aspettavo un'eccezione", this.isExceptionExpected);
            }
        } catch (Exception e) {
            Assert.assertTrue("Mi aspettavo un'eccezione: " + e.getMessage(), this.isExceptionExpected);
        }
    }

}
