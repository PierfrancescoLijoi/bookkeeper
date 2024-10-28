package org.apache.bookkeeper.bookie;

import org.apache.bookkeeper.util.IOUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;

import static org.mockito.Mockito.*;

@RunWith(Parameterized.class)
public class FileInfoIntegrationTest {

    private FileInfo fileInfo;
    private File fileNew;
    private boolean exception;
    private String result;
    private long size;
    private int sizeExpected;
    private boolean deleted;

    enum ParamFile {
        NULL, INVALID, VALID, SAME, FC_NULL,
    }

    enum ParamSize {
        MAX, MIN
    }

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {ParamFile.SAME, ParamSize.MAX},
                {ParamFile.FC_NULL, ParamSize.MAX},
                {ParamFile.INVALID, ParamSize.MAX}, // New parameter for closed file
                {ParamFile.VALID, ParamSize.MAX}   // New parameter for empty file
        });
    }

    void configure(ParamFile fileType, ParamSize sizeType) throws IOException {
        File fl = createTempFile("testFileInfo");
        this.result = "mocked";
        String masterkey = "";
        this.fileInfo = new FileInfo(fl, masterkey.getBytes(), 0);

        this.deleted = false;
        this.exception = false;

        switch (fileType) {
            case SAME:
                this.fileNew = fl;
                ByteBuffer bb[] = new ByteBuffer[1];
                bb[0] = ByteBuffer.wrap(this.result.getBytes());
                this.fileInfo.write(bb, 0);
                break;
            case FC_NULL:
                File f = mock(File.class);
                Mockito.when(f.delete()).thenReturn(true);
                Mockito.when(f.getPath()).thenReturn("/tmp/integrationTest");
                this.fileInfo = new FileInfo(f, "".getBytes(), 0);
                this.fileNew = createTempFile("mockedAAAA");
                break;
            case INVALID: // New case for closed file
                File closedFile = mock(File.class);
                Mockito.when(closedFile.canRead()).thenReturn(false); // Simulating a closed file
                this.fileInfo = new FileInfo(closedFile, masterkey.getBytes(), 0);
                this.fileNew = createTempFile("closedFile");
                break;
            case VALID: // New case for empty file
                this.fileNew = createTempFile("emptyFile");
                break;
        }

        switch (sizeType) {
            case MAX:
                this.size = Long.MAX_VALUE;
                this.sizeExpected = this.result.length();
                break;
            case MIN:
                this.size = 0; // Setting to zero for minimum size case
                this.sizeExpected = 0; // Expect no content
                break;
        }
    }

    public FileInfoIntegrationTest(ParamFile fileType, ParamSize sizeType) {
        try {
            configure(fileType, sizeType);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private File createTempFile(String suffix) throws IOException {
        return IOUtils.createTempFileAndDeleteOnExit("bookie", suffix);
    }

    @Test
    public void testMoveToNewLocation() {
        try {
            this.fileInfo.moveToNewLocation(this.fileNew, this.size);
            Assert.assertEquals(this.deleted, fileInfo.isDeleted());
        } catch (Exception e) {
            Assert.assertTrue("mi aspettavo true " + e.getMessage(), this.exception);
        }
    }

    @Test
    public void testMoveToNewLocationWithMockito1() throws IOException {
        // Create a mock of FileInfo
        FileInfo mockFileInfo = mock(FileInfo.class);
        File mockNewFile = mock(File.class);

        // Simulate the behavior of moveToNewLocation
        doNothing().when(mockFileInfo).moveToNewLocation(any(File.class), anyLong());

        // Call the method
        mockFileInfo.moveToNewLocation(mockNewFile, 1024L);

        // Verify that moveToNewLocation was called with the correct parameters
        verify(mockFileInfo, times(1)).moveToNewLocation(mockNewFile, 1024L);
    }

    @Test
    public void testMoveToNewLocationDoesNotThrowIOExceptionWhenRenamingToExistingFile() throws IOException {

        File newFile = createTempFile("newFileForTesting");
        newFile.createNewFile();

        try {
            this.fileInfo.moveToNewLocation(newFile, this.size);
        } catch (IOException e) {
            // In this case we do not expect an IOException to be thrown
            Assert.fail("Non ci si aspettava un'eccezione IOException, ma è stata sollevata: " + e.getMessage());
        }

        // Verify that the destination file was not created, because it already existed
        Assert.assertTrue("Il file di destinazione non dovrebbe essere stato rinominato.", newFile.exists());
    }


}