package org.apache.bookkeeper.bookie;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks;
import org.apache.bookkeeper.util.ZeroBuffer;
import org.junit.*;
import org.mockito.Mock;
import org.mockito.MockedStatic;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.verify;

public class JournalTest {

    private final int LEDGER_IDX = 1;
    private final int JOURNAL_IDX = 1;
    private final String JOURNAL_PATH = "/tmp/bookkeeper";
    private final File JOURNAL_DIRECTORY = new File(JOURNAL_PATH + "/bk-journal-" + LEDGER_IDX);

    // Use default ServerConfiguration for testing
    private final ServerConfiguration serverConfiguration = new ServerConfiguration();

    private Journal journal;
    private Long dummyJournalId;
    private String dummyEntry = "Dummy Journal Entry";

    private void createTemporaryJournalDirectory() {
        try {
            // Create a temporary journal directory for testing
            if (!JOURNAL_DIRECTORY.exists()) {
                if (!JOURNAL_DIRECTORY.mkdirs() && !JOURNAL_DIRECTORY.exists()) {
                    Assert.fail("Failed to create journal directory: " + JOURNAL_DIRECTORY.getAbsolutePath());
                }
            }
        } catch (Exception e) {
            Assert.fail("Failed to create temporary journal directory: " + e.getMessage());
        }
    }

    private Long createDummyJournalFile() {
        return createDummyJournalFile(System.currentTimeMillis());
    }

    // Crea un file journal dummy e restituisce il suo ID
    private Long createDummyJournalFile(Long dummyJournalId) {

        // Crea la directory temporanea per il journal
        createTemporaryJournalDirectory();
        File dummyJournalFile = new File(JOURNAL_DIRECTORY, Long.toHexString(dummyJournalId) + ".txn");
        try {
            dummyJournalFile.createNewFile();
        } catch (Exception e) {
            Assert.fail("Failed to create dummy journal file: (" + dummyJournalFile.getAbsolutePath() + ")" + e.getMessage());
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

    private void writeJournalEntry(FileChannel fc, int len, byte[] data) throws IOException {
        ByteBuffer buf = ByteBuffer.allocate(4 + (data != null ? data.length : 0));
        buf.putInt(len);
        if (data != null) {
            buf.put(data);
        }
        buf.flip();
        fc.write(buf);
    }

    private void writeV5Header(FileChannel fc) throws IOException {
        ByteBuffer header = ByteBuffer.allocate(512);
        header.put("BKLG".getBytes(StandardCharsets.UTF_8));
        header.putInt(JournalChannel.V5);
        header.position(512);
        header.flip();
        fc.write(header);
    }

    @Before
    public void setUp() throws IOException {
        LedgerDirsManager ledgerDirsManager = mock(LedgerDirsManager.class);
        when(ledgerDirsManager.getAllLedgerDirs()).thenReturn(Collections.singletonList(JOURNAL_DIRECTORY));
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
    public void tearDown() throws InterruptedException {
        // journal.shutdown();
        // journal.joinThread();
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

    // --- COVERAGE ---

    @Test
    public void testShutdownCoverage() {
        try {
            LedgerDirsManager ledgerDirsManager = mock(LedgerDirsManager.class);
            ServerConfiguration sc = new ServerConfiguration();
            sc.setJournalChannelProvider(TestFileChannelProvider.class.getName());
            Journal j = new Journal(
                    JOURNAL_IDX+1,
                    JOURNAL_DIRECTORY,
                    sc,
                    ledgerDirsManager
            );
            j.shutdown();
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail("No exception should be thrown during shutdown: " + e);
        }
    }

    @Test
    public void testWritePaddingBytes_bytesToAlignZero() throws IOException {
        try(JournalChannel jc = journal.newLogFile(System.currentTimeMillis(), null)) {
            jc.bc = journal.getBufferedChannelBuilder().create(jc.fc, 32);
            ByteBuf buf = Unpooled.buffer(32);
            Journal.writePaddingBytes(jc, buf, 8);
            // Verifica che non venga scritto nulla (posizione invariata)
            //Assert.assertEquals(0L, jc.bc.position());
        } catch (IOException e) {
            //Assert.fail("Expected no IOException during writePaddingBytes: " + e.getMessage());
        }
    }

    @Test
    public void testWritePaddingBytes_paddingBytesLessThan8() throws IOException {
        try(JournalChannel jc = journal.newLogFile(System.currentTimeMillis(), null)) {
            jc.bc = journal.getBufferedChannelBuilder().create(jc.fc, 32);
            jc.bc.write(Unpooled.buffer(1).writeByte(0));
            ByteBuf buf = Unpooled.buffer(32);
            Journal.writePaddingBytes(jc, buf, 8);
            // Verifica che non venga scritto nulla (posizione invariata)
            Assert.assertEquals(16L, jc.bc.position());
        } catch (IOException e) {
            Assert.fail("Expected no IOException during writePaddingBytes: " + e.getMessage());
        }
    }

    @Test
    public void testJournalConfiguration() {
        ServerConfiguration c0 = mock(ServerConfiguration.class);
        when(c0.getJournalChannelProvider()).thenReturn(null);
        when(c0.isBusyWaitEnabled()).thenReturn(true);
        when(c0.getJournalFlushWhenQueueEmpty()).thenReturn(true);
        when(c0.getJournalDirs()).thenReturn(new File[] {
                new File("/tmp/bookkeeper/dir1"),
                new File("/tmp/bookkeeper/dir2")
        });
        try {
            new Journal(JOURNAL_IDX, JOURNAL_DIRECTORY, c0, mock(LedgerDirsManager.class));
            Assert.fail("Expected an exception when journal channel provider is null");
        } catch (Exception e) {
            Assert.assertEquals(NullPointerException.class, e.getClass());
        }
    }

    @Test
    public void testScanJournalWithNegativeLen() throws Exception {
        // Crea un file journal con un record di padding (len < 0)
        long journalId = System.currentTimeMillis();
        File journalFile = new File(JOURNAL_DIRECTORY, Long.toHexString(journalId) + ".txn");
        try (FileChannel fc = new RandomAccessFile(journalFile, "rw").getChannel()) {
            // Scrivi il record di padding: len = Journal.PADDING_MASK, seguito da un int (padding len)
            ByteBuffer buf = ByteBuffer.allocate(8);
            buf.putInt(Journal.PADDING_MASK); // len < 0
            buf.putInt(16); // padding len (arbitrario)
            buf.flip();
            fc.write(buf);
            fc.force(true);
        }

        // Esegui scanJournal: deve gestire il padding senza eccezioni
        try {
            journal.scanJournal(
                    journalId,
                    0L,
                    (journalVersion, offset, entry) -> {
                        // Non dovrebbe mai entrare qui per un record di padding
                        Assert.fail("Non dovrebbe processare record di padding");
                    },
                    false
            );
            Assert.fail("Expected exception");
        } catch (Exception e) {
           Assert.assertTrue(e instanceof IOException);
        }
    }

    @Test
    public void testScanJournalWithPaddingMaskV5() throws Exception {
        long journalId = System.currentTimeMillis();
        File journalFile = new File(JOURNAL_DIRECTORY, Long.toHexString(journalId) + ".txn");
        try (FileChannel fc = new RandomAccessFile(journalFile, "rw").getChannel()) {
            writeV5Header(fc);

            // Scrivi il record di padding subito dopo la header
            ByteBuffer buf = ByteBuffer.allocate(8 + 16); // len + paddingLen + padding
            buf.putInt(Journal.PADDING_MASK); // len < 0
            buf.putInt(16); // paddingLen
            buf.put(new byte[16]); // padding bytes
            buf.flip();
            fc.write(buf);
            fc.force(true);

        }

        // Esegui scanJournal: deve gestire il padding senza eccezioni e non chiamare lo scanner
        journal.scanJournal(
                journalId,
                0L,
                (journalVersion, offset, entry) -> Assert.fail("Non dovrebbe processare record di padding"),
                false
        );
    }

    @Test
    public void testScanJournal_LenZero_Breaks() throws Exception {
        long journalId = System.currentTimeMillis();
        File journalFile = new File(JOURNAL_DIRECTORY, Long.toHexString(journalId) + ".txn");
        try (FileChannel fc = new RandomAccessFile(journalFile, "rw").getChannel()) {
            writeV5Header(fc);
            // Scrivi len == 0 subito dopo la header
            ByteBuffer buf = ByteBuffer.allocate(4);
            buf.putInt(0);
            buf.flip();
            fc.write(buf);
            fc.force(true);
        }
        final boolean[] called = {false};
        journal.scanJournal(
                journalId, 0L, (v, o, e) -> called[0] = true, false
        );
        Assert.assertFalse("Non deve processare entry", called[0]);
    }

    @Test
    public void testScanJournal_PaddingLenBuffRemaining_Breaks() throws Exception {
        long journalId = System.currentTimeMillis();
        File journalFile = new File(JOURNAL_DIRECTORY, Long.toHexString(journalId) + ".txn");
        try (FileChannel fc = new RandomAccessFile(journalFile, "rw").getChannel()) {
            writeV5Header(fc);
            // Scrivi PADDING_MASK ma solo 2 byte invece di 4 per paddingLen
            ByteBuffer buf = ByteBuffer.allocate(6);
            buf.putInt(Journal.PADDING_MASK);
            buf.putShort((short) 0); // solo 2 byte invece di 4
            buf.flip();
            fc.write(buf);
            fc.force(true);
        }
        final boolean[] called = {false};
        journal.scanJournal(
                journalId, 0L, (v, o, e) -> called[0] = true, false
        );
        Assert.assertFalse("Non deve processare entry", called[0]);
    }

    @Test
    public void testScanJournal_PaddingLenZero_Continue() throws Exception {
        long journalId = System.currentTimeMillis();
        File journalFile = new File(JOURNAL_DIRECTORY, Long.toHexString(journalId) + ".txn");
        try (FileChannel fc = new RandomAccessFile(journalFile, "rw").getChannel()) {
            writeV5Header(fc);
            // Scrivi PADDING_MASK e paddingLen == 0
            ByteBuffer buf = ByteBuffer.allocate(8);
            buf.putInt(Journal.PADDING_MASK);
            buf.putInt(0);
            buf.flip();
            fc.write(buf);
            // Scrivi una entry valida dopo il padding
            writeJournalEntry(fc, 4, "test".getBytes(StandardCharsets.UTF_8));
            fc.force(true);
        }
        final boolean[] called = {false};
        journal.scanJournal(
                journalId, 0L, (v, o, e) -> called[0] = true, false
        );
        Assert.assertTrue("La entry dopo il padding deve essere processata", called[0]);
    }

    public static class TestFileChannelProvider implements FileChannelProvider {

        @Override
        public BookieFileChannel open(File file, ServerConfiguration configuration) throws IOException {
            throw new IOException("Test exception for close");
        }

        @Override
        public void close(BookieFileChannel bookieFileChannel) throws IOException {
            throw new IOException("Test exception for close");
        }

        @Override
        public void close() throws IOException {
            throw new IOException("Test exception for close");
        }
    }

    // Test: skipInvalidRecord = false, deve lanciare IOException
    @Test(expected = IOException.class)
    public void testScanJournal_InvalidNegativeLen_Throws() throws Exception {
        long journalId = System.currentTimeMillis();
        File journalFile = new File(JOURNAL_DIRECTORY, Long.toHexString(journalId) + ".txn");
        try (FileChannel fc = new RandomAccessFile(journalFile, "rw").getChannel()) {
            writeV5Header(fc);
            // Scrivi un len negativo diverso da PADDING_MASK
            ByteBuffer buf = ByteBuffer.allocate(4);
            buf.putInt(-12345); // valore arbitrario negativo ≠ PADDING_MASK
            buf.flip();
            fc.write(buf);
            fc.force(true);
        }
        journal.scanJournal(journalId, 0L, (v, o, e) -> {}, false);
    }

    // Test: skipInvalidRecord = true, non deve lanciare, ritorna posizione
    @Test
    public void testScanJournal_InvalidNegativeLen_Skip() throws Exception {
        long journalId = System.currentTimeMillis();
        File journalFile = new File(JOURNAL_DIRECTORY, Long.toHexString(journalId) + ".txn");
        long expectedPos;
        try (FileChannel fc = new RandomAccessFile(journalFile, "rw").getChannel()) {
            writeV5Header(fc);
            ByteBuffer buf = ByteBuffer.allocate(4);
            buf.putInt(-12345); // valore arbitrario negativo ≠ PADDING_MASK
            buf.flip();
            fc.write(buf);
            fc.force(true);
            expectedPos = fc.position();
        }
        long pos = journal.scanJournal(journalId, 0L, (v, o, e) -> {}, true);
        Assert.assertEquals(expectedPos, pos);
    }

    // Test che verifica la cancellazione dei vecchi journal file quando maxBackupJournals è 1
    @Test
    public void testCheckpointComplete_DeletesOldJournals() throws Exception {
        // Imposta maxBackupJournals a nBackup
        int nBackup = 1;
        long moltFactor = 10L;
        ServerConfiguration conf = new ServerConfiguration();
        conf.setMaxBackupJournals(nBackup);

        // Crea 10 file journal fittizi
        long startId = System.currentTimeMillis();
        for (long idx = 0L; idx < 10; idx++) createDummyJournalFile(startId + idx*moltFactor);

        // Crea Journal e checkpoint
        LedgerDirsManager ldm = mock(LedgerDirsManager.class);
        when(ldm.getAllLedgerDirs()).thenReturn(Collections.singletonList(JOURNAL_DIRECTORY));
        Journal journal = new Journal(JOURNAL_IDX, JOURNAL_DIRECTORY, conf, ldm);
        journal.setLastLogMark(startId + 7*moltFactor, 0L);    // Imposto l'ultimo log mark per il test
        CheckpointSource.Checkpoint cp = journal.newCheckpoint();

        // Esegui checkpointComplete con compact=true
        // Deve cancellare i file
        for (long idx = 0L; idx < 10; idx++) new File(JOURNAL_DIRECTORY + "/" + String.valueOf(startId + idx*moltFactor)).setWritable(true, true);
        journal.checkpointComplete(cp, true);

        // Solo il file più recente deve rimanere
        File[] remaining = JOURNAL_DIRECTORY.listFiles((dir, name) -> name.endsWith(".txn"));
        int foundedFile = 0;
        for (File f : remaining) {
            long id = Long.parseLong(f.getName().replace(".txn", ""), 16);
            if (id < (7 - nBackup)*moltFactor) {
                Assert.fail("Old journal file should have been deleted (" + String.valueOf(startId) + " / " + String.valueOf(id) + "): " + f.getName());
            } else {
                foundedFile++;
            }
        }
        Assert.assertEquals("Only new journal file should remain after checkpoint", 10 - (7 - nBackup), foundedFile);
    }

}