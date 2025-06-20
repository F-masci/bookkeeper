package org.apache.bookkeeper.bookie;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks;
import org.junit.*;
import org.mockito.Mock;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.mock;

public class JournalTest {

    private static final int LEDGER_IDX = 1;
    private static final int JOURNAL_IDX = 1;
    private static final String JOURNAL_PATH = "/tmp/bookkeeper";
    private static final File JOURNAL_DIRECTORY = new File(JOURNAL_PATH + "/bk-journal-" + LEDGER_IDX);

    @Mock
    private LedgerDirsManager ledgerDirsManager = mock(LedgerDirsManager.class);

    // Use default ServerConfiguration for testing
    private final ServerConfiguration serverConfiguration = new ServerConfiguration();

    private Journal journal;
    private Long dummyJournalId;
    private String dummyEntry = "Dummy Journal Entry";

    private void createTemporaryJournalDirectory() {
        try {
            // Create a temporary journal directory for testing
            JOURNAL_DIRECTORY.mkdirs();
        } catch (Exception e) {
            Assert.fail("Failed to create temporary journal directory: " + e.getMessage());
        }
    }

    private Long createDummyJournalFile() {

        // Crea la directory temporanea per il journal
        createTemporaryJournalDirectory();

        // Crea un file journal dummy e restituisce il suo ID
        Long dummyJournalId = System.currentTimeMillis();
        try {
            File dummyJournalFile = new File(JOURNAL_DIRECTORY, Long.toHexString(dummyJournalId) + ".txn");
            if (!dummyJournalFile.createNewFile()) {
                Assert.fail("Failed to create dummy journal file: " + dummyJournalFile.getAbsolutePath());
            }
        } catch (Exception e) {
            Assert.fail("Failed to create dummy journal file: " + e.getMessage());
        }
        return dummyJournalId;
    }

    private void writeToDummyJournalFile() throws IOException {
        File dummyJournalFile = new File(JOURNAL_DIRECTORY, Long.toHexString(dummyJournalId) + ".txn");
        FileChannel fc = new RandomAccessFile(dummyJournalFile, "rw").getChannel();
        ByteBuffer buf = ByteBuffer.wrap(dummyEntry.getBytes());
        try {
            fc.write(buf);
            fc.force(true);
        } catch (Exception e) {
            Assert.fail("Failed to write dummy journal file: " + e.getMessage());
        } finally {
            fc.close();
        }
    }

    @Before
    public void setUp() throws IOException {

        this.journal = new Journal(
            JOURNAL_IDX,
            JOURNAL_DIRECTORY,
            serverConfiguration,
            ledgerDirsManager
        );

        // Create a temporary journal directory for testing
        this.dummyJournalId = createDummyJournalFile();

    }

    @After
    public void tearDown() {
        // Clean up the temporary journal directory after tests
        if (JOURNAL_DIRECTORY.exists()) {
            for (File file : JOURNAL_DIRECTORY.listFiles()) {
                file.delete();
            }
            JOURNAL_DIRECTORY.delete();
        }
    }

    @Test
    public void testInvalidDirectoryRetrivialJournalIds() {

        // Search in a non-existing directory
        try {
            List<Long> journalIds = Journal.listJournalIds(new File(JOURNAL_PATH + "/not-found"), null);
            Assert.assertEquals(Collections.emptyList(), journalIds);
        } catch (Exception e) {
            Assert.fail("Expected no exception for non-existing directory");
        }

        // Search in a file instead of a directory
        try {
            List<Long> journalIds = Journal.listJournalIds(new File(JOURNAL_PATH+ "/journal.txt"), null);
            Assert.assertEquals(Collections.emptyList(), journalIds);
        } catch (Exception e) {
            Assert.fail("Expected no exception for file instead of directory");
        }

    }

    @Test
    public void testEmptyDirectoryRetrivialJournalIds() {

        // Create an empty directory for testing
        if (JOURNAL_DIRECTORY.exists()) {
            for (File file : JOURNAL_DIRECTORY.listFiles()) {
                file.delete();
            }
        }

        // Retrieve journal IDs from the empty directory
        try {
            List<Long> journalIds = Journal.listJournalIds(JOURNAL_DIRECTORY, null);
            Assert.assertEquals(Collections.emptyList(), journalIds);
        } catch (Exception e) {
            Assert.fail("Expected no exception for empty directory: " + e.getMessage());
        }
    }

    @Test
    public void testValidDirectoryRetrivialJournalIds() {

        // Create a file in the journal directory
        Long invalidFileId = 0L;
        try {
            File invalidFile = new File(JOURNAL_DIRECTORY, String.valueOf(invalidFileId) + "-dummy.txt");
            if (!invalidFile.createNewFile()) Assert.fail("Failed to create invalid file in journal directory");
        } catch (IOException e) {
            Assert.fail("Failed to create invalid file in journal directory: " + e.getMessage());
        }

        // Retrieve journal IDs from the valid directory
        try {
            List<Long> journalIds = Journal.listJournalIds(JOURNAL_DIRECTORY, null);
            Assert.assertTrue(journalIds.contains(dummyJournalId));
        } catch (Exception e) {
            Assert.fail("Expected no exception for valid directory: " + e.getMessage());
        }

        // Use a valid filter to retrieve journal IDs
        try {
            List<Long> journalIds = Journal.listJournalIds(JOURNAL_DIRECTORY, new Journal.JournalIdFilter() {
                @Override
                public boolean accept(long jId) {
                    return jId == dummyJournalId;
                }
            });
            Assert.assertTrue(journalIds.contains(dummyJournalId));
        } catch (Exception e) {
            Assert.fail("Expected no exception for valid directory with filter: " + e.getMessage());
        }

        // Use filter that does not match any journal ID
        try {
            List<Long> journalIds = Journal.listJournalIds(JOURNAL_DIRECTORY, new Journal.JournalIdFilter() {
                @Override
                public boolean accept(long jId) {
                    return jId == -1; // No journal should match this ID
                }
            });
            Assert.assertTrue(journalIds.isEmpty());
        } catch (Exception e) {
            Assert.fail("Expected no exception for valid directory with non-matching filter: " + e.getMessage());
        }

        // Retrieve journal IDs from the directory with an invalid file
        try {
            List<Long> journalIds = Journal.listJournalIds(JOURNAL_DIRECTORY, null);
            Assert.assertFalse(journalIds.contains(invalidFileId));
        } catch (Exception e) {
            Assert.fail("Expected no exception for directory with invalid file: " + e.getMessage());
        }

    }

    @Test
    public void testInvalidExtensionInDirectoryRetrivialJournalIds() {

        // Create a file in the journal directory
        Long invalidFileId = 0L;
        try {
            File invalidFile = new File(JOURNAL_DIRECTORY, String.valueOf(invalidFileId) + "-dummy.txt");
            if (!invalidFile.createNewFile()) {
                Assert.fail("Failed to create invalid file in journal directory");
            }
        } catch (IOException e) {
            Assert.fail("Failed to create invalid file in journal directory: " + e.getMessage());
        }

        // Retrieve journal IDs from the directory with an invalid file
        try {
            List<Long> journalIds = Journal.listJournalIds(JOURNAL_DIRECTORY, null);
            Assert.assertFalse(journalIds.contains(invalidFileId));
        } catch (Exception e) {
            Assert.fail("Expected no exception for directory with invalid file: " + e.getMessage());
        }
    }

    @Test
    public void testValidJournalRead() throws IOException {
        this.writeToDummyJournalFile();
        try {
            long r = journal.scanJournal(
                    dummyJournalId,
                    0L,
                    new Journal.JournalScanner() {
                        @Override
                        public void process(int journalVersion, long offset, ByteBuffer entry) throws IOException {
                            Assert.assertEquals(ByteBuffer.wrap(dummyEntry.getBytes()), entry);
                        }
                    },
                    false
            );
            Assert.assertTrue("Expected to read journal entry", r > 0);
        } catch (Exception e) {
            Assert.fail("Expected no exception during journal read: " + e.getMessage());
        }
    }

    @Test
    public void testNegativeLongJournalRead() throws IOException {
        this.writeToDummyJournalFile();
        try {
            // Not found journal ID
            journal.scanJournal(
                    -1L,
                    -1L,
                    new Journal.JournalScanner() {
                        @Override
                        public void process(int journalVersion, long offset, ByteBuffer entry) throws IOException {
                            Assert.fail("Expected no processing for not found journal ID");
                        }
                    },
                    true
            );
        } catch (Exception e) {
            Assert.fail("Expected no exception during journal read: " + e.getMessage());
        }
    }

    @Test
    public void testBigPosJournalRead() throws IOException {

        this.writeToDummyJournalFile();

        // Calculate file size of the dummy journal file
        File dummyJournalFile = new File(JOURNAL_DIRECTORY, Long.toHexString(dummyJournalId) + ".txn");
        long fileSize = dummyJournalFile.length();

        try {
            // Attempt to read beyond the file size
            journal.scanJournal(
                    dummyJournalId,
                    fileSize+1, // Offset beyond the file size
                    new Journal.JournalScanner() {
                        @Override
                        public void process(int journalVersion, long offset, ByteBuffer entry) throws IOException {
                            Assert.fail("Expected no processing for offset beyond file size");
                        }
                    },
                    true
            );
        } catch (Exception e) {
            Assert.fail("Expected no exception during journal read: " + e.getMessage());
        }
    }

    // Test: ledgerId e entryId validi, entry non vuoto, ackBeforeSync true, callback valida, ctx valido
    @Test
    public void testValidWrite() throws Exception {
        try {
            ByteBuf entry = Unpooled.wrappedBuffer(dummyEntry.getBytes(StandardCharsets.UTF_8));
            TestWriteCallback cb = new TestWriteCallback();
            String ctx = "testCtx";
            journal.logAddEntry(1L, 1L, entry, true, cb, ctx);
        } catch (Exception e) {
            Assert.fail("Expected no exception during valid write: " + e.getMessage());
        }
    }

    // Test: ledgerId negativo, entryId negativo, entry non vuoto, ackBeforeSync false, callback valida, ctx valido
    @Test
    public void testLogAddEntry_NegativeIds_NonEmptyEntry_AckBeforeSyncFalse_CallbackValid_CtxValid() throws Exception {
        try {
            ByteBuf entry = Unpooled.wrappedBuffer(dummyEntry.getBytes(StandardCharsets.UTF_8));
            TestWriteCallback cb = new TestWriteCallback();
            String ctx = "testCtx";
            journal.logAddEntry(-1L, -1L, entry, true, cb, ctx);
        } catch (Exception e) {
            Assert.fail("Expected no exception during valid write: " + e.getMessage());
        }
    }

    // Test: ledgerId massimo, entryId massimo, entry non vuoto, ackBeforeSync true, callback valida, ctx valido
    @Test
    public void testLogAddEntry_MaxIds_NonEmptyEntry_AckBeforeSyncTrue_CallbackValid_CtxValid() throws Exception {
        try {
            ByteBuf entry = Unpooled.wrappedBuffer(dummyEntry.getBytes(StandardCharsets.UTF_8));
            TestWriteCallback cb = new TestWriteCallback();
            journal.logAddEntry(Long.MAX_VALUE, Long.MAX_VALUE, entry, true, cb, "maxCtx");
        } catch (Exception e) {
            Assert.fail("Expected no exception during valid write: " + e.getMessage());
        }
    }

    // Test: ledgerId valido, entryId valido, entry vuoto, ackBeforeSync false, callback valida, ctx valido
    @Test
    public void testLogAddEntry_ValidIds_EmptyEntry_AckBeforeSyncFalse_CallbackValid_CtxValid() throws Exception {
        try {
            ByteBuf entry = Unpooled.wrappedBuffer(dummyEntry.getBytes(StandardCharsets.UTF_8));
            TestWriteCallback cb = new TestWriteCallback();
            String ctx = "testCtx";
            journal.logAddEntry(4L, 4L, entry, false, cb, ctx);
        } catch (Exception e) {
            Assert.fail("Expected no exception during valid write: " + e.getMessage());
        }
    }

    // Test: ledgerId valido, entryId valido, entry non vuoto, ackBeforeSync true, callback valida, ctx null
    @Test
    public void testLogAddEntry_ValidIds_NonEmptyEntry_AckBeforeSyncTrue_CallbackValid_CtxNull() throws Exception {
        try {
            ByteBuf entry = Unpooled.wrappedBuffer(dummyEntry.getBytes(StandardCharsets.UTF_8));
            TestWriteCallback cb = new TestWriteCallback();
            String ctx = "testCtx";
            journal.logAddEntry(4L, 5L, entry, true, cb, null);
        } catch (Exception e) {
            Assert.fail("Expected no exception during valid write: " + e.getMessage());
        }
    }

    // Callback di test per verificare l’invocazione
    private static class TestWriteCallback implements BookkeeperInternalCallbacks.WriteCallback {
        volatile boolean invoked = false;
        volatile Object ctx = null;
        private final CountDownLatch latch = new CountDownLatch(1);

        @Override
        public void writeComplete(int rc, long ledgerId, long entryId, BookieId addr, Object ctx) {
            this.invoked = true;
            this.ctx = ctx;
            latch.countDown();
        }

        void await() throws InterruptedException {
            latch.await(2, TimeUnit.SECONDS);
        }
    }

}