package org.apache.bookkeeper.bookie.storage.ldb;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)
public class GetWriteCacheTest {

    private WriteCache writeCache;
    private ByteBufAllocator byteBufAllocator;
    private ByteBuf entry;
    private final long ledgerId = 2;
    private final long entryId = 1;
    private final boolean isValidLedgerId;
    private final boolean isValidEntryId;
    private final boolean isExceptionExpected;

    @Parameterized.Parameters
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][] {
                { Boolean.TRUE, Boolean.TRUE, Boolean.FALSE },
                { Boolean.TRUE, Boolean.FALSE, Boolean.TRUE },
                { Boolean.FALSE, Boolean.TRUE, Boolean.TRUE },
                { Boolean.FALSE, Boolean.FALSE, Boolean.TRUE }
        });
    }

    public GetWriteCacheTest(boolean isValidLedgerId, boolean isValidEntryId, boolean isExceptionExpected) {
        this.isValidLedgerId = isValidLedgerId;
        this.isValidEntryId = isValidEntryId;
        this.isExceptionExpected = isExceptionExpected;
    }

    @Before
    public void setUp() {
        byteBufAllocator = ByteBufAllocator.DEFAULT;
        int entryNumber = 10;
        int entrySize = 1024;
        writeCache = new WriteCache(byteBufAllocator, entrySize * entryNumber);

        entry = Unpooled.wrappedBuffer("bytes into the entry".getBytes());
        writeCache.put(ledgerId, entryId, entry);
    }

    @After
    public void tearDown() {
        writeCache.clear();
        entry.release();
        writeCache.close();
    }

    @Test
    public void testGetFromCache() {
        long testLedgerId = isValidLedgerId ? ledgerId : ledgerId + 1;
        long testEntryId = isValidEntryId ? entryId : entryId + 1;

        try {
            ByteBuf result = writeCache.get(testLedgerId, testEntryId);
            if (isExceptionExpected) {
                Assert.assertNull(result);
            } else {
                Assert.assertEquals(entry, result);
            }
        } catch (Exception e) {
            Assert.assertTrue("Exception was expected", isExceptionExpected);
        }
    }

}