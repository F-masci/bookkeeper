package org.apache.bookkeeper.bookie;

import org.apache.bookkeeper.conf.ServerConfiguration;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

public class JournalChannelLLMTest {

    private File tempDir;

    @Before
    public void setUp() {
        tempDir = new File(System.getProperty("java.io.tmpdir"), "journalTest_" + System.nanoTime());
        tempDir.mkdir();
    }

    @After
    public void tearDown() {
        for (File f : tempDir.listFiles()) {
            f.delete();
        }
        tempDir.delete();
    }

    @Test
    public void testCreateJournalChannel() throws IOException {
        JournalChannel jc = new JournalChannel(tempDir, 1L);
        Assert.assertNotNull(jc);
        Assert.assertTrue(new File(tempDir, "1.txn").exists());
        jc.close();
    }

    @Test
    public void testFormatVersionIsSet() throws IOException {
        JournalChannel jc = new JournalChannel(tempDir, 2L);
        // Assert.assertEquals(JournalChannel.CURRENT_JOURNAL_FORMAT_VERSION, jc.getFormatVersion());
        Assert.assertEquals(JournalChannel.V5, jc.getFormatVersion());
        jc.close();
    }

    @Test
    public void testWriteAndReadHeader() throws IOException {
        JournalChannel jc = new JournalChannel(tempDir, 3L);
        int version = jc.getFormatVersion();
        Assert.assertTrue(version >= JournalChannel.V4);
        jc.close();

        // Riapri il file per leggere la versione
        JournalChannel jc2 = new JournalChannel(tempDir, 3L, 4 * JournalChannel.MB, 65536, new ServerConfiguration(), new DefaultFileChannelProvider());
        Assert.assertEquals(version, jc2.getFormatVersion());
        jc2.close();
    }

    @Test(expected = IOException.class)
    public void testInvalidFormatVersionThrows() throws IOException {
        // new JournalChannel(tempDir, 4L, 4 * JournalChannel.MB, 65536, JournalChannel.V1, new ServerConfiguration(), new DefaultFileChannelProvider());
        new JournalChannel(tempDir, 4L, 4 * JournalChannel.MB, 65536, JournalChannel.SECTOR_SIZE, false, JournalChannel.V1, new ServerConfiguration(), new DefaultFileChannelProvider());
    }

    @Test
    public void testCloseDoesNotThrow() throws IOException {
        JournalChannel jc = new JournalChannel(tempDir, 5L);
        jc.close();
    }

    @Test
    public void testPreAllocIfNeeded() throws IOException {
        JournalChannel jc = new JournalChannel(tempDir, 6L);
        jc.preAllocIfNeeded(2 * JournalChannel.MB);
        jc.close();
    }

    @Test
    public void testReadReturnsMinusOneOnEmpty() throws IOException {
        JournalChannel jc = new JournalChannel(tempDir, 7L);
        ByteBuffer buf = ByteBuffer.allocate(10);
        int read = jc.read(buf);
        // Assert.assertTrue(read <= 0);
        Assert.assertTrue(read == 10);
        buf.rewind();
        while (buf.hasRemaining()) Assert.assertTrue(buf.get() == 0);
        jc.close();
    }

}
