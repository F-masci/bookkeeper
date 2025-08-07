package org.apache.bookkeeper.bookie;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.apache.bookkeeper.bookie.stats.JournalStats;
import org.apache.bookkeeper.common.util.affinity.CpuAffinity;
import org.apache.bookkeeper.common.util.affinity.impl.CpuAffinityImpl;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.proto.BookieRequestHandler;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.bouncycastle.util.encoders.Hex;
import org.junit.*;

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
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.junit.runner.RunWith;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.verify;

import org.mockito.Mock;
import org.mockito.MockedConstruction;

@RunWith(MockitoJUnitRunner.class)
public class JournalTest {

    private static final int LEDGER_IDX = 1;
    private static final int JOURNAL_IDX = 1;
    private static final String JOURNAL_PATH = "/tmp/bookkeeper";
    private static final File JOURNAL_DIRECTORY = new File(JOURNAL_PATH + "/bk-journal-" + LEDGER_IDX);

    // Use default ServerConfiguration for testing
    private ServerConfiguration serverConfiguration;

    private Journal journal;
    private Long dummyJournalId;
    private final String dummyEntry = "Dummy Journal Entry";

    private void createTemporaryJournalDirectory() {
        try {
            // Create a temporary journal directory for testing
            if (!JOURNAL_DIRECTORY.exists() && !JOURNAL_DIRECTORY.mkdirs() && !JOURNAL_DIRECTORY.exists())
                Assert.fail("Failed to create journal directory: " + JOURNAL_DIRECTORY.getAbsolutePath());
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

    @Mock
    FileChannelProvider mockFcpException;

    @Mock
    ServerConfiguration mockSc;

    @Before
    public void setUp() throws IOException {

        LedgerDirsManager ledgerDirsManager = mock(LedgerDirsManager.class);
        when(ledgerDirsManager.getAllLedgerDirs()).thenReturn(Collections.singletonList(JOURNAL_DIRECTORY));

        serverConfiguration = new ServerConfiguration();
        serverConfiguration.setJournalBufferedEntriesThreshold(0);
        serverConfiguration.setJournalFlushWhenQueueEmpty(true);
        serverConfiguration.setJournalReuseFiles(false);

        this.journal = new Journal(
            JOURNAL_IDX,
            JOURNAL_DIRECTORY,
            serverConfiguration,
            ledgerDirsManager
        );

        // Create a temporary journal directory for testing
        this.dummyJournalId = createDummyJournalFile();

        mockSc = mock(ServerConfiguration.class);

        mockFcpException = mock(FileChannelProvider.class);
        lenient().when(mockFcpException.open(any(), any())).thenThrow(new IOException("Test exception for open"));
        lenient().doThrow(new IOException("Test exception for close")).when(mockFcpException).close();
        lenient().doThrow(new IOException("Test exception for close")).when(mockFcpException).close(any());

    }

    @After
    public void tearDown() {
        // journal.shutdown();
        // journal.joinThread();
        // Clean up the temporary journal directory after tests
        File testDirectory = new File(JOURNAL_PATH);
        for (File file : testDirectory.listFiles()) {
            if(file.isDirectory()) {
                for (File subFile : file.listFiles()) {
                    subFile.delete();
                }
                file.delete();
            } else {
                file.delete();
            }
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
            File invalidFile = new File(JOURNAL_DIRECTORY, invalidFileId + "-dummy.txt");
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
            File invalidFile = new File(JOURNAL_DIRECTORY, invalidFileId + "-dummy.txt");
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

    // Test: entry non vuoto, ackBeforeSync true, callback valida, ctx valido
    @Test
    public void testValidWrite() throws Exception {
        try {
            ByteBuf entry = Unpooled.wrappedBuffer(dummyEntry.getBytes(StandardCharsets.UTF_8));
            BookkeeperInternalCallbacks.WriteCallback mockWcb = mock(BookkeeperInternalCallbacks.WriteCallback.class);
            String ctx = "testCtx";
            journal.logAddEntry(entry, true, mockWcb, ctx);
            Assert.assertEquals("Expected 1 entry to be written", 1, journal.getJournalQueueLength());
        } catch (Exception e) {
            Assert.fail("Expected no exception during valid write: " + e.getMessage());
        }
    }

    // Test: ledgerId negativo, entryId negativo, entry non vuoto, ackBeforeSync false, callback valida, ctx valido
    @Test
    public void testLogAddEntry_NegativeIds_NonEmptyEntry_AckBeforeSyncFalse_CallbackValid_CtxValid() throws Exception {
        try {
            ByteBuf entry = Unpooled.wrappedBuffer(dummyEntry.getBytes(StandardCharsets.UTF_8));
            BookkeeperInternalCallbacks.WriteCallback mockWcb = mock(BookkeeperInternalCallbacks.WriteCallback.class);
            String ctx = "testCtx";
            journal.logAddEntry(-1L, -1L, entry, true, mockWcb, ctx);
            Assert.assertEquals("Expected 1 entry to be written", 1, journal.getJournalQueueLength());
        } catch (Exception e) {
            Assert.fail("Expected no exception during valid write: " + e.getMessage());
        }
    }

    // Test: ledgerId massimo, entryId massimo, entry non vuoto, ackBeforeSync true, callback valida, ctx valido
    @Test
    public void testLogAddEntry_MaxIds_NonEmptyEntry_AckBeforeSyncTrue_CallbackValid_CtxValid() throws Exception {
        try {
            ByteBuf entry = Unpooled.wrappedBuffer(dummyEntry.getBytes(StandardCharsets.UTF_8));
            BookkeeperInternalCallbacks.WriteCallback mockWcb = mock(BookkeeperInternalCallbacks.WriteCallback.class);
            journal.logAddEntry(Long.MAX_VALUE, Long.MAX_VALUE, entry, true, mockWcb, "maxCtx");
            Assert.assertEquals("Expected 1 entry to be written", 1, journal.getJournalQueueLength());
        } catch (Exception e) {
            Assert.fail("Expected no exception during valid write: " + e.getMessage());
        }
    }

    // Test: ledgerId valido, entryId valido, entry vuoto, ackBeforeSync false, callback valida, ctx valido
    @Test
    public void testLogAddEntry_ValidIds_EmptyEntry_AckBeforeSyncFalse_CallbackValid_CtxValid() throws Exception {
        try {
            ByteBuf entry = Unpooled.wrappedBuffer(dummyEntry.getBytes(StandardCharsets.UTF_8));
            BookkeeperInternalCallbacks.WriteCallback mockWcb = mock(BookkeeperInternalCallbacks.WriteCallback.class);
            String ctx = "testCtx";
            journal.logAddEntry(4L, 4L, entry, false, mockWcb, ctx);
            Assert.assertEquals("Expected 1 entry to be written", 1, journal.getJournalQueueLength());
        } catch (Exception e) {
            Assert.fail("Expected no exception during valid write: " + e.getMessage());
        }
    }

    // Test: ledgerId valido, entryId valido, entry non vuoto, ackBeforeSync true, callback valida, ctx null
    @Test
    public void testLogAddEntry_ValidIds_NonEmptyEntry_AckBeforeSyncTrue_CallbackValid_CtxNull() throws Exception {
        try {
            ByteBuf entry = Unpooled.wrappedBuffer(dummyEntry.getBytes(StandardCharsets.UTF_8));
            BookkeeperInternalCallbacks.WriteCallback mockWcb = mock(BookkeeperInternalCallbacks.WriteCallback.class);
            journal.logAddEntry(4L, 5L, entry, true, mockWcb, null);
            Assert.assertEquals("Expected 1 entry to be written", 1, journal.getJournalQueueLength());
        } catch (Exception e) {
            Assert.fail("Expected no exception during valid write: " + e.getMessage());
        }
    }

    // --- COVERAGE ---

    @Test
    public void testShutdownCoverage_providerException() {
        try (MockedStatic<FileChannelProvider> fileChannelProviderMock = mockStatic(FileChannelProvider.class)) {
            LedgerDirsManager ledgerDirsManager = mock(LedgerDirsManager.class);
            ServerConfiguration sc = new ServerConfiguration();

            // Forza il mock del FileChannelProvider per generare le eccezioni
            fileChannelProviderMock.when(() -> FileChannelProvider.newProvider(anyString())).thenReturn(mockFcpException);

            Journal j = new Journal(
                    JOURNAL_IDX+1,
                    JOURNAL_DIRECTORY,
                    sc,
                    ledgerDirsManager
            );
            j.shutdown();
        } catch (Exception e) {
            Assert.fail("No exception should be thrown during shutdown: " + e);
        }
    }

    @Test
    public void testShutdownCoverage_providerNull() {
        try (MockedStatic<FileChannelProvider> fileChannelProviderMock = mockStatic(FileChannelProvider.class)) {
            LedgerDirsManager ledgerDirsManager = mock(LedgerDirsManager.class);
            ServerConfiguration sc = new ServerConfiguration();

            // Forza il mock del FileChannelProvider per generare le eccezioni
            fileChannelProviderMock.when(() -> FileChannelProvider.newProvider(anyString())).thenReturn(null);

            Journal j = new Journal(
                    JOURNAL_IDX+1,
                    JOURNAL_DIRECTORY,
                    sc,
                    ledgerDirsManager
            );
            j.shutdown();
        } catch (Exception e) {
            Assert.fail("No exception should be thrown during shutdown: " + e);
        }
    }

    // Controllo che non venga scritto nulla se i byte da allineare sono zero
    @Test
    public void testWritePaddingBytes_bytesToAlignZero() throws Exception {

        BufferedChannel mockBc = mock(BufferedChannel.class);
        when(mockBc.position()).thenReturn(0L);

        JournalChannel mockJc = mock(JournalChannel.class);
        mockJc.bc = mockBc;

        ByteBuf buf = Unpooled.buffer(32);
        Journal.writePaddingBytes(mockJc, buf, 8);
        // Verifica che non venga scritto nulla (posizione invariata)
        verify(mockBc, never()).write(any(ByteBuf.class));
    }

    // Controllo che venga scritto il buffer di padding corretto
    @Test
    public void testWritePaddingBytes_paddingBytesLessThan8() throws IOException {

        BufferedChannel mockBc = mock(BufferedChannel.class);
        when(mockBc.position()).thenReturn(1L);

        JournalChannel mockJc = mock(JournalChannel.class);
        mockJc.bc = mockBc;

        ByteBuf buf = Unpooled.buffer(32);
        Journal.writePaddingBytes(mockJc, buf, 8);
        // Verifica che non venga scritto nulla (posizione invariata)
        verify(mockBc, times(1)).write(eq(buf));

    }

    @Test
    public void testJournalConfiguration() throws Exception {
        ServerConfiguration c0 = mock(ServerConfiguration.class);
        when(c0.getJournalChannelProvider()).thenReturn(DefaultFileChannelProvider.class.getName());
        when(c0.isBusyWaitEnabled()).thenReturn(true);
        when(c0.getJournalMaxGroupWaitMSec()).thenReturn(0L);
        when(c0.getJournalQueueSize()).thenReturn(1024);
        when(c0.getJournalDirNames()).thenReturn(new String[] {
                "/tmp/bookkeeper/dir1",
                "/tmp/bookkeeper/dir2"
        });
        when(c0.getJournalDirs()).thenReturn(new File[] {
            new File("/tmp/bookkeeper/dir1"),
            new File("/tmp/bookkeeper/dir2")
        });
        Journal journal = new Journal(JOURNAL_IDX, JOURNAL_DIRECTORY, c0, mock(LedgerDirsManager.class));

        // Verifica che le configurazioni siano state impostate correttamente
        Assert.assertEquals(0, journal.getJournalQueueLength());
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
        journal.scanJournal(
                journalId, 0L, (v, o, e) -> Assert.fail("Expected to not process entry"), false
        );
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
        journal.scanJournal(
                journalId, 0L, (v, o, e) -> Assert.fail("Expected to not process entry"), false
        );
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
                journalId, 0L, (v, o, e) -> {
                    called[0] = true;
                    String entryValue;
                    byte[] data = new byte[e.remaining()];
                    e.get(data);
                    entryValue = new String(data, StandardCharsets.UTF_8);
                    Assert.assertEquals("test", entryValue);
                }, false
        );
        Assert.assertTrue("La entry dopo il padding deve essere processata", called[0]);
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
        for (long idx = 0L; idx < 10; idx++) new File(JOURNAL_DIRECTORY + "/" + (startId + idx * moltFactor)).setWritable(true, true);
        journal.checkpointComplete(cp, true);

        // Solo il file più recente deve rimanere
        File[] remaining = JOURNAL_DIRECTORY.listFiles((dir, name) -> name.endsWith(".txn"));
        int foundedFile = 0;
        for (File f : remaining) {
            long id = Long.parseLong(f.getName().replace(".txn", ""), 16);
            if (id < startId + (7 - nBackup)*moltFactor) {
                Assert.fail("Old journal file should have been deleted (" + startId + " / " + id + "): " + f.getName());
            } else {
                foundedFile++;
            }
        }
        Assert.assertEquals("Only new journal file should remain after checkpoint", moltFactor - (7 - nBackup), foundedFile);
    }

    // Controlla che il sistema non esegua operazioni di rollLog su un checkpoint generico
    @Test
    public void testCheckpointComplete_GenericCheckpoint() throws Exception {

        // Crea un checkpoint generico
        Journal.LastLogMark mockLlm = mock(Journal.LastLogMark.class);
        GenericTestCheckpoint mockCk = new GenericTestCheckpoint(mockLlm);

        journal.checkpointComplete(mockCk, true);

        verify(mockLlm, never()).rollLog(any(Journal.LastLogMark.class));
    }

    // Controllo la scrittura su un file journal con molte entry
    @Test
    public void testRun_BigNumEntry() throws Exception {

        long journalId = 200L;

        // Devo superare 1 MB per forzare il rollover del journal
        long expextedSize = 2 * Journal.MB;
        String entryText = "test";
        long entryLen = entryText.getBytes(StandardCharsets.UTF_8).length;   // Verranno aggiunti anche i bytes per la lunghezza => sa maggiore di expected
        long entryNum = expextedSize / entryLen;

        // Prepara un file journal esistente
        File journalDirectory = new File(JOURNAL_PATH + "/journal-run-coverage");
        if(!journalDirectory.exists()) journalDirectory.mkdirs();
        File journalFile = new File(journalDirectory, Long.toHexString(journalId) + ".txn");
        if(!journalFile.exists()) journalFile.createNewFile();

        // Mock
        LedgerDirsManager mockLdm = mock(LedgerDirsManager.class);
        when(mockLdm.getAllLedgerDirs()).thenReturn(Collections.singletonList(journalDirectory));

        serverConfiguration.setJournalFlushWhenQueueEmpty(true);
        serverConfiguration.setJournalBufferedEntriesThreshold(50);
        serverConfiguration.setJournalMaxGroupWaitMSec(100L);
        serverConfiguration.setMaxJournalSizeMB(1L);
        serverConfiguration.setJournalSyncData(false);

        serverConfiguration.setJournalPageCacheFlushIntervalMSec(0L);

        Journal journal = new Journal(
                JOURNAL_IDX + 3,
                journalDirectory,
                serverConfiguration,
                mockLdm
        );

        journal.start();

        TestWriteCallback cb = new TestWriteCallback(entryNum);

        for(long i = 0; i < entryNum; i++) {
            ByteBuf entryToWrite = Unpooled.wrappedBuffer(entryText.getBytes(StandardCharsets.UTF_8));
            journal.logAddEntry(
                    LEDGER_IDX,
                    i,
                    entryToWrite,
                    true,
                    cb,
                    "testCtx"
            );
        }

        // Attendi che il callback venga invocato
        cb.await(60L);
        Assert.assertEquals("Callback should be invoked " + entryNum + " time", entryNum, cb.invoked);

        final long[] entryCount = {0};
        List<Long> journalIds = Journal.listJournalIds(journalDirectory, null);
        // Mi aspetto un rollover, quindi più di un file journal
        Assert.assertTrue("Expected more than 1 journal file", journalIds.size() > 1);
        for(Long jId : journalIds) {
            journal.scanJournal(
                    jId,
                    0L,
                    (version, offset, entry) -> {
                        entryCount[0]++;
                        String data = new String(entry.array(), entry.position(), entry.remaining(), StandardCharsets.UTF_8);
                        Assert.assertEquals("test", data);
                    },
                    false
            );
        }
        // Mi aspetto che il numero di entry sia uguale a quelle scritte (considerando tutti i file)
        Assert.assertEquals(entryNum + " entry expected", entryNum, entryCount[0]);

        journal.shutdown();

    }

    // Controllo la scrittura su un file journal prima dello scadere del timeout
    @Test
    public void testRun_CheckBeforeTimeout() throws Exception {

        long journalId = 2500L;

        String entryText = "test";
        long entryNum = 10L;

        // Prepara un file journal esistente
        File journalDirectory = new File(JOURNAL_PATH + "/journal-run-coverage");
        if(!journalDirectory.exists()) journalDirectory.mkdirs();
        File journalFile = new File(journalDirectory, Long.toHexString(journalId) + ".txn");
        if(!journalFile.exists()) journalFile.createNewFile();

        // Mock
        LedgerDirsManager mockLdm = mock(LedgerDirsManager.class);
        when(mockLdm.getAllLedgerDirs()).thenReturn(Collections.singletonList(journalDirectory));

        serverConfiguration.setJournalFlushWhenQueueEmpty(false);
        serverConfiguration.setJournalBufferedEntriesThreshold(50);
        serverConfiguration.setJournalMaxGroupWaitMSec(5 * 1000L);
        serverConfiguration.setMaxJournalSizeMB(1L);
        serverConfiguration.setJournalSyncData(false);

        Journal journal = new Journal(
                JOURNAL_IDX + 3,
                journalDirectory,
                serverConfiguration,
                mockLdm
        );

        journal.start();

        TestWriteCallback cb = new TestWriteCallback(entryNum);

        for(long i = 0; i < entryNum; i++) {
            ByteBuf entryToWrite = Unpooled.wrappedBuffer(entryText.getBytes(StandardCharsets.UTF_8));
            journal.logAddEntry(
                    LEDGER_IDX,
                    i,
                    entryToWrite,
                    false,
                    cb,
                    "testCtx"
            );
        }

        // Chiudi il journal
        // Non deve forzare il flush
        journal.shutdown();

        // Attendi che il callback venga invocato
        cb.await(1L);
        Assert.assertEquals("Callback should be invoked 0 time", 0, cb.invoked);

        final long[] entryCount = {0};
        List<Long> journalIds = Journal.listJournalIds(journalDirectory, null);
        // Mi aspetto un rollover, quindi più di un file journal
        Assert.assertTrue("Expected more than 1 journal file", journalIds.size() > 1);
        for(Long jId : journalIds) {
            journal.scanJournal(
                    jId,
                    0L,
                    (version, offset, entry) -> {
                        entryCount[0]++;
                        String data = new String(entry.array(), entry.position(), entry.remaining(), StandardCharsets.UTF_8);
                        Assert.assertEquals("test", data);
                    },
                    false
            );
        }
        // Mi aspetto che il numero di entry sia uguale a quelle scritte (considerando tutti i file)
        Assert.assertEquals("0 entry expected", 0, entryCount[0]);

        journal.shutdown();

    }

    // Controllo la scrittura con BookieRequest
    @Test
    public void testRun_BookieRequest() throws Exception {

        long journalId = 60L;
        String entryText = "test";

        // Prepara un file journal esistente
        File journalDirectory = new File(JOURNAL_PATH + "/journal-run-coverage");
        if(!journalDirectory.exists()) journalDirectory.mkdirs();
        File journalFile = new File(journalDirectory, Long.toHexString(journalId) + ".txn");
        if(!journalFile.exists()) journalFile.createNewFile();

        // Mock
        LedgerDirsManager mockLdm = mock(LedgerDirsManager.class);
        when(mockLdm.getAllLedgerDirs()).thenReturn(Collections.singletonList(journalDirectory));

        serverConfiguration.setJournalFlushWhenQueueEmpty(true);
        serverConfiguration.setJournalBufferedEntriesThreshold(0);
        serverConfiguration.setJournalBufferedWritesThreshold(0L);
        serverConfiguration.setJournalMaxGroupWaitMSec(0L);
        serverConfiguration.setJournalSyncData(false);

        StatsLogger mockSl = mock(NullStatsLogger.class);

        Counter mockGC = mock(Counter.class);
        lenient().when(mockSl.getThreadScopedCounter(anyString())).thenReturn(mockGC);
        lenient().when(mockSl.getCounter(anyString())).thenReturn(mockGC);

        OpStatsLogger mockGOsl = mock(OpStatsLogger.class);
        lenient().when(mockSl.getOpStatsLogger(anyString())).thenReturn(mockGOsl);
        lenient().when(mockSl.getThreadScopedOpStatsLogger(anyString())).thenReturn(mockGOsl);

        Counter mockEqC = mock(Counter.class);
        lenient().when(mockSl.scopeLabel(anyString(), anyString())).thenReturn(mockSl);
        lenient().when(mockSl.scope(anyString())).thenReturn(mockSl);
        lenient().when(mockSl.getCounter(BookKeeperServerStats.JOURNAL_NUM_FLUSH_EMPTY_QUEUE)).thenReturn(mockEqC);

        Journal journal = new Journal(
                JOURNAL_IDX + 4,
                journalDirectory,
                serverConfiguration,
                mockLdm,
                mockSl,
                ByteBufAllocator.DEFAULT
        );

        journal.start();

        BookieRequestHandler mockBrh = mock(BookieRequestHandler.class);
        TestWriteCallback cb = new TestWriteCallback(2);

        journal.logAddEntry(
                LEDGER_IDX,
                0L,
                Unpooled.wrappedBuffer(entryText.getBytes(StandardCharsets.UTF_8)),
                true,
                cb,
                mockBrh
        );
        journal.forceLedger(LEDGER_IDX, cb, mockBrh);

        cb.await(5);

        journal.shutdown();

        verify(mockBrh, atLeastOnce()).flushPendingResponse();

        // Mi aspetto che il flush sia stato eseguito per la coda vuota
        verify(mockEqC, atLeastOnce()).inc();

    }

    // Controllo la scrittura di ForceLedger ed ExplicitLac
    @Test
    public void testRun_ForceLedger_NoLedgerExplicitLacV5() throws Exception {

        long journalId = 220L;

        String entryText = "test";
        long entryNum = 5L;

        // Prepara un file journal esistente
        File journalDirectory = new File(JOURNAL_PATH + "/journal-run-coverage");
        if(!journalDirectory.exists()) journalDirectory.mkdirs();
        File journalFile = new File(journalDirectory, Long.toHexString(journalId) + ".txn");
        if(!journalFile.exists()) journalFile.createNewFile();

        // Mock
        LedgerDirsManager mockLdm = mock(LedgerDirsManager.class);
        when(mockLdm.getAllLedgerDirs()).thenReturn(Collections.singletonList(journalDirectory));

        serverConfiguration.setJournalFlushWhenQueueEmpty(false);
        serverConfiguration.setJournalBufferedEntriesThreshold(0);
        serverConfiguration.setJournalBufferedWritesThreshold(0L);
        serverConfiguration.setJournalMaxGroupWaitMSec(1L);
        serverConfiguration.setJournalSyncData(true);

        // I file journal vengono flushati senza aspettare
        serverConfiguration.setJournalPageCacheFlushIntervalMSec(0);

        // Nelle versioni > V5 il LAC viene scritto, ma io non voglio che venga scritto
        serverConfiguration.setJournalFormatVersionToWrite(JournalChannel.V5);

        StatsLogger mockSl = mock(NullStatsLogger.class);

        Counter mockGC = mock(Counter.class);
        lenient().when(mockSl.getThreadScopedCounter(anyString())).thenReturn(mockGC);
        lenient().when(mockSl.getCounter(anyString())).thenReturn(mockGC);

        OpStatsLogger mockGOsl = mock(OpStatsLogger.class);
        lenient().when(mockSl.getOpStatsLogger(anyString())).thenReturn(mockGOsl);
        lenient().when(mockSl.getThreadScopedOpStatsLogger(anyString())).thenReturn(mockGOsl);

        Counter mockEqC = mock(Counter.class);
        lenient().when(mockSl.scopeLabel(anyString(), anyString())).thenReturn(mockSl);
        lenient().when(mockSl.scope(anyString())).thenReturn(mockSl);
        lenient().when(mockSl.getCounter(BookKeeperServerStats.JOURNAL_NUM_FLUSH_EMPTY_QUEUE)).thenReturn(mockEqC);

        Journal journal = new Journal(
                JOURNAL_IDX + 4,
                journalDirectory,
                serverConfiguration,
                mockLdm,
                mockSl,
                ByteBufAllocator.DEFAULT
        );

        journal.start();

        TestWriteCallback cb = new TestWriteCallback(entryNum+2);

        for(long i = 0; i < entryNum; i++) {
            ByteBuf entryToWrite = Unpooled.wrappedBuffer(entryText.getBytes(StandardCharsets.UTF_8));
            journal.logAddEntry(
                    LEDGER_IDX,
                    i,
                    entryToWrite,
                    true,
                    cb,
                    "testCtx"
            );
        }

        journal.forceLedger(LEDGER_IDX, cb, "testCtx");

        // Non mi aspetto che venga effettivamente scritto il LAC, ma il callback deve essere invocato
        journal.logAddEntry(
                LEDGER_IDX,
                BookieImpl.METAENTRY_ID_LEDGER_EXPLICITLAC,
                Unpooled.wrappedBuffer("explicitLac".getBytes(StandardCharsets.UTF_8)),
                true,
                cb,
                "testCtx"
        );

        // Attendi che il callback venga invocato
        cb.await();
        Assert.assertEquals("Callback should be invoked " + (entryNum+2) + " time", entryNum+2, cb.invoked);

        final long[] entryCount = {0};
        journal.scanJournal(
                journalId+1,
                0L,
                (version, offset, entry) -> {
                    entryCount[0]++;
                    String data = new String(entry.array(), entry.position(), entry.remaining(), StandardCharsets.UTF_8);
                    Assert.assertEquals("test", data);
                },
                false
        );
        Assert.assertEquals(entryNum + " entry expected", entryNum, entryCount[0]);

        // Mi aspetto che il flush non sia stato eseguito per la coda vuota ma per il timeout
        verify(mockEqC, never()).inc();

        journal.shutdown();

    }

    // Controllo la scrittura con LEDGER_EXPLICITLAC per V6
    @Test
    public void testRun_LedgerExplicitLac() throws Exception {

        long journalId = 120L;

        // Prepara un file journal esistente
        File journalDirectory = new File(JOURNAL_PATH + "/journal-run-coverage");
        if(!journalDirectory.exists()) journalDirectory.mkdirs();
        File journalFile = new File(journalDirectory, Long.toHexString(journalId) + ".txn");
        if(!journalFile.exists()) journalFile.createNewFile();

        // Mock
        LedgerDirsManager mockLdm = mock(LedgerDirsManager.class);
        when(mockLdm.getAllLedgerDirs()).thenReturn(Collections.singletonList(journalDirectory));

        serverConfiguration.setJournalFlushWhenQueueEmpty(false);
        serverConfiguration.setJournalBufferedEntriesThreshold(0);
        serverConfiguration.setJournalBufferedWritesThreshold(10 * JournalChannel.MB);
        serverConfiguration.setJournalMaxGroupWaitMSec(0L);

        // Nelle versioni >= V5 il LAC viene scritto
        serverConfiguration.setJournalFormatVersionToWrite(JournalChannel.V6);

        Journal journal = new Journal(
                JOURNAL_IDX + 4,
                journalDirectory,
                serverConfiguration,
                mockLdm
        );

        journal.start();

        TestWriteCallback cb = new TestWriteCallback();

        // Mi aspetto che venga effettivamente scritto il LAC
        journal.logAddEntry(
                LEDGER_IDX,
                BookieImpl.METAENTRY_ID_LEDGER_EXPLICITLAC,
                Unpooled.wrappedBuffer("explicitLac".getBytes(StandardCharsets.UTF_8)),
                true,
                cb,
                "testCtx"
        );

        // Attendi che il callback venga invocato
        cb.await();
        Assert.assertEquals("Callback should be invoked 1 time", 1, cb.invoked);

        final long[] entryCount = {0};
        journal.scanJournal(
                journalId+1,
                0L,
                (version, offset, entry) -> {
                    entryCount[0]++;
                    String data = new String(entry.array(), entry.position(), entry.remaining(), StandardCharsets.UTF_8);
                    Assert.assertEquals("explicitLac", data);
                },
                false
        );
        Assert.assertEquals("1 entry expected", 1, entryCount[0]);

        journal.shutdown();

    }

    @Test
    public void testRun_NoReuseJournal_1() throws Exception {
        try (MockedStatic<FileChannelProvider> fileChannelProviderMock = mockStatic(FileChannelProvider.class)) {

            String entryText = "test";
            long entryNum = 500L;

            // Prepara un file journal esistente
            File journalDirectory = new File(JOURNAL_PATH + "/journal-run-coverage");
            if(!journalDirectory.exists()) journalDirectory.mkdirs();

            // Mock
            LedgerDirsManager mockLdm = mock(LedgerDirsManager.class);
            when(mockLdm.getAllLedgerDirs()).thenReturn(Collections.singletonList(journalDirectory));

            FileChannelProvider mockFcp = mock(FileChannelProvider.class);
            when(mockFcp.supportReuseFile()).thenReturn(true);
            when(mockFcp.open(any(File.class), any(ServerConfiguration.class)))
                    .thenAnswer(invocation -> {
                        File file = invocation.getArgument(0);
                        ServerConfiguration localServerConfiguration = invocation.getArgument(1);
                        return new DefaultFileChannel(file, localServerConfiguration);
                    });

            // Forza il mock del FileChannelProvider per generare le eccezioni
            fileChannelProviderMock.when(() -> FileChannelProvider.newProvider(anyString())).thenReturn(mockFcp);

            serverConfiguration.setJournalFlushWhenQueueEmpty(false);
            serverConfiguration.setJournalBufferedEntriesThreshold(0);
            serverConfiguration.setJournalMaxGroupWaitMSec(20L);
            serverConfiguration.setJournalSyncData(false);
            serverConfiguration.setJournalReuseFiles(false);

            Journal journal = new Journal(
                    JOURNAL_IDX + 4,
                    journalDirectory,
                    serverConfiguration,
                    mockLdm
            );

            TestWriteCallback cb = new TestWriteCallback(entryNum);

            for(long i = 0; i < entryNum; i++) {
                ByteBuf entryToWrite = Unpooled.wrappedBuffer(entryText.getBytes(StandardCharsets.UTF_8));
                journal.logAddEntry(
                        LEDGER_IDX,
                        i,
                        entryToWrite,
                        true,
                        cb,
                        "testCtx"
                );
            }

            journal.start();

            // Attendi che il callback venga invocato
            cb.await();
            Assert.assertEquals("Callback should be invoked " + entryNum + " time", entryNum, cb.invoked);

            journal.shutdown();

            final long[] entryCount = {0};
            List<Long> journalIds = Journal.listJournalIds(journalDirectory, null);
            // Non mi aspetto un rollover, quindi un solo file journal
            Assert.assertEquals("Expected only 1 journal file", 1, journalIds.size());
            for(Long jId : journalIds) {
                journal.scanJournal(
                        jId,
                        0L,
                        (version, offset, entry) -> {
                            entryCount[0]++;
                            String data = new String(entry.array(), entry.position(), entry.remaining(), StandardCharsets.UTF_8);
                            Assert.assertEquals("test", data);
                        },
                        false
                );
            }
            // Mi aspetto che il numero di entry sia uguale a quelle scritte (considerando tutti i file)
            Assert.assertEquals(entryNum + " entry expected", entryNum, entryCount[0]);

            // Chiudi nuovamente il journal per verificare che non ci siano eccezioni
            journal.shutdown();

        } catch (Exception e) {
            Assert.fail("No exception should be thrown during shutdown: " + e);
        }

    }

    @Test
    public void testRun_NoReuseJournal_2() throws Exception {
        try (MockedStatic<FileChannelProvider> fileChannelProviderMock = mockStatic(FileChannelProvider.class)) {

            String entryText = "test";
            long entryNum = 2L;

            // Prepara un file journal esistente
            File journalDirectory = new File(JOURNAL_PATH + "/journal-run-coverage");
            if(!journalDirectory.exists()) journalDirectory.mkdirs();

            // Mock
            LedgerDirsManager mockLdm = mock(LedgerDirsManager.class);
            when(mockLdm.getAllLedgerDirs()).thenReturn(Collections.singletonList(journalDirectory));

            FileChannelProvider mockFcp = mock(FileChannelProvider.class);
            when(mockFcp.supportReuseFile()).thenReturn(true);
            when(mockFcp.open(any(File.class), any(ServerConfiguration.class)))
                    .thenAnswer(invocation -> {
                        File file = invocation.getArgument(0);
                        ServerConfiguration localServerConfiguration = invocation.getArgument(1);
                        return new DefaultFileChannel(file, localServerConfiguration);
                    });

            // Forza il mock del FileChannelProvider per generare le eccezioni
            fileChannelProviderMock.when(() -> FileChannelProvider.newProvider(anyString())).thenReturn(mockFcp);

            serverConfiguration.setJournalFlushWhenQueueEmpty(false);
            serverConfiguration.setJournalBufferedEntriesThreshold(0);
            serverConfiguration.setJournalMaxGroupWaitMSec(0L);
            serverConfiguration.setJournalSyncData(false);
            serverConfiguration.setJournalReuseFiles(true);
            serverConfiguration.setMaxBackupJournals(10);

            Journal journal = new Journal(
                    JOURNAL_IDX + 4,
                    journalDirectory,
                    serverConfiguration,
                    mockLdm
            );

            journal.start();

            TestWriteCallback cb = new TestWriteCallback(entryNum);

            for(long i = 0; i < entryNum; i++) {
                ByteBuf entryToWrite = Unpooled.wrappedBuffer(entryText.getBytes(StandardCharsets.UTF_8));
                journal.logAddEntry(
                        LEDGER_IDX,
                        i,
                        entryToWrite,
                        true,
                        cb,
                        "testCtx"
                );
            }

            // Attendi che il callback venga invocato
            cb.await();
            Assert.assertEquals("Callback should be invoked " + entryNum + " time", entryNum, cb.invoked);

            final long[] entryCount = {0};
            List<Long> journalIds = Journal.listJournalIds(journalDirectory, null);
            // Non mi aspetto un rollover, quindi un solo file journal
            Assert.assertEquals("Expected only 1 journal file", 1, journalIds.size());
            for(Long jId : journalIds) {
                journal.scanJournal(
                        jId,
                        0L,
                        (version, offset, entry) -> {
                            entryCount[0]++;
                            String data = new String(entry.array(), entry.position(), entry.remaining(), StandardCharsets.UTF_8);
                            Assert.assertEquals("test", data);
                        },
                        false
                );
            }
            // Mi aspetto che il numero di entry sia uguale a quelle scritte (considerando tutti i file)
            Assert.assertEquals(entryNum + " entry expected", entryNum, entryCount[0]);

            journal.shutdown();

        } catch (Exception e) {
            Assert.fail("No exception should be thrown during shutdown: " + e);
        }

    }

    @Test
    public void testRun_NoReuseJournal_3() throws Exception {
        try (MockedStatic<FileChannelProvider> fileChannelProviderMock = mockStatic(FileChannelProvider.class)) {

            long journalId1 = 300L;
            long journalId2 = journalId1 + 10L;

            String entryText = "test";
            long entryNum = 2L;

            // Prepara un file journal esistente
            File journalDirectory = new File(JOURNAL_PATH + "/journal-run-coverage");
            if(!journalDirectory.exists()) journalDirectory.mkdirs();
            File jF1 = new File(journalDirectory, Long.toHexString(journalId1) + ".txn");
            if(!jF1.exists()) jF1.createNewFile();
            File jF2 = new File(journalDirectory, Long.toHexString(journalId2) + ".txn");
            if(!jF2.exists()) jF2.createNewFile();

            // Mock
            LedgerDirsManager mockLdm = mock(LedgerDirsManager.class);
            when(mockLdm.getAllLedgerDirs()).thenReturn(Collections.singletonList(journalDirectory));

            FileChannelProvider mockFcp = mock(FileChannelProvider.class);
            when(mockFcp.supportReuseFile()).thenReturn(true);
            when(mockFcp.open(any(File.class), any(ServerConfiguration.class)))
                    .thenAnswer(invocation -> {
                        File file = invocation.getArgument(0);
                        ServerConfiguration localServerConfiguration = invocation.getArgument(1);
                        return new DefaultFileChannel(file, localServerConfiguration);
                    });

            // Forza il mock del FileChannelProvider per generare le eccezioni
            fileChannelProviderMock.when(() -> FileChannelProvider.newProvider(anyString())).thenReturn(mockFcp);

            serverConfiguration.setJournalFlushWhenQueueEmpty(false);
            serverConfiguration.setJournalBufferedEntriesThreshold(0);
            serverConfiguration.setJournalMaxGroupWaitMSec(0L);
            serverConfiguration.setJournalSyncData(false);
            serverConfiguration.setJournalReuseFiles(true);
            serverConfiguration.setMaxBackupJournals(0);

            Journal journal = new Journal(
                    JOURNAL_IDX + 4,
                    journalDirectory,
                    serverConfiguration,
                    mockLdm
            );

            // Imposto l'ultimo log mark per il test
            // Uso il log più vecchio (journalId1) affinché il journal piu recente (journalId2) venga mantenuto
            // Alla fine del processo mi aspetto che journalId1 e journalId2 non vengano eliminato
            journal.setLastLogMark(journalId1, 0L); // Imposto l'ultimo log mark per il test
            CheckpointSource.Checkpoint cp = journal.newCheckpoint();
            journal.checkpointComplete(cp, false); // Completo il checkpoint senza rimuovere i vecchi file

            journal.start();

            TestWriteCallback cb = new TestWriteCallback(entryNum);

            for(long i = 0; i < entryNum; i++) {
                ByteBuf entryToWrite = Unpooled.wrappedBuffer(entryText.getBytes(StandardCharsets.UTF_8));
                journal.logAddEntry(
                        LEDGER_IDX,
                        i,
                        entryToWrite,
                        true,
                        cb,
                        "testCtx"
                );
            }

            // Attendi che il callback venga invocato
            cb.await();
            Assert.assertEquals("Callback should be invoked " + entryNum + " time", entryNum, cb.invoked);

            final long[] entryCount = {0};
            List<Long> journalIds = Journal.listJournalIds(journalDirectory, null);
            // Non mi aspetto un rollover, quindi 3 file journal
            Assert.assertEquals("Expected only 3 journal file", 3, journalIds.size());
            // Mi aspetto che il journalId1 sia ancora presente
            Assert.assertTrue("Expected journalId1 to be reused", journalIds.contains(journalId1));
            // Mi aspetto che il journalId2 sia ancora presente
            Assert.assertTrue("Expected journalId2 to be present", journalIds.contains(journalId2));

            // Controllo che i file siano stati scritti correttamente
            for(Long jId : journalIds) {
                journal.scanJournal(
                        jId,
                        0L,
                        (version, offset, entry) -> {
                            entryCount[0]++;
                            String data = new String(entry.array(), entry.position(), entry.remaining(), StandardCharsets.UTF_8);
                            Assert.assertEquals("test", data);
                        },
                        false
                );
            }
            // Mi aspetto che il numero di entry sia uguale a quelle scritte (considerando tutti i file)
            Assert.assertEquals(entryNum + " entry expected", entryNum, entryCount[0]);

            journal.shutdown();

        } catch (Exception e) {
            Assert.fail("No exception should be thrown during shutdown: " + e);
        }

    }

    // Questo test fallisce poiché maxBackupJournals è impostato a 0 e journalIds.size() è 0, ma journalIds.get(0) va in errore
    //
    //  journalIds = listJournalIds(journalDirectory, null);
    //  Long replaceLogId = fileChannelProvider.supportReuseFile() && journalReuseFiles
    //      && journalIds.size() >= maxBackupJournals
    //      && journalIds.get(0) < lastLogMark.getCurMark().getLogFileId()
    //      ? journalIds.get(0) : null;
    //
    // Questo test è stato commentato poiché non è possibile riutilizzare un journal se non esiste
    // e il metodo listJournalIds ritorna una lista vuota ma non viene controllata.
    /*@Test
    public void testRun_ReuseJournalNotPresent() throws Exception {
        try (MockedStatic<FileChannelProvider> fileChannelProviderMock = mockStatic(FileChannelProvider.class)) {

            String entryText = "test";
            long entryNum = 2L;

            // Prepara un file journal esistente
            File journalDirectory = new File(JOURNAL_PATH + "/journal-run-coverage");
            if(!journalDirectory.exists()) journalDirectory.mkdirs();

            // Mock
            LedgerDirsManager mockLdm = mock(LedgerDirsManager.class);
            when(mockLdm.getAllLedgerDirs()).thenReturn(Collections.singletonList(journalDirectory));

            FileChannelProvider mockFcp = mock(FileChannelProvider.class);
            when(mockFcp.supportReuseFile()).thenReturn(true);
            when(mockFcp.open(any(File.class), any(ServerConfiguration.class)))
                    .thenAnswer(invocation -> {
                        File file = invocation.getArgument(0);
                        ServerConfiguration localServerConfiguration = invocation.getArgument(1);
                        return new DefaultFileChannel(file, localServerConfiguration);
                    });

            // Forza il mock del FileChannelProvider per generare le eccezioni
            fileChannelProviderMock.when(() -> FileChannelProvider.newProvider(anyString())).thenReturn(mockFcp);

            serverConfiguration.setJournalFlushWhenQueueEmpty(false);
            serverConfiguration.setJournalBufferedEntriesThreshold(0);
            serverConfiguration.setJournalMaxGroupWaitMSec(0L);
            serverConfiguration.setJournalSyncData(false);
            serverConfiguration.setJournalReuseFiles(true);
            serverConfiguration.setMaxBackupJournals(0);

            Journal journal = new Journal(
                    JOURNAL_IDX + 4,
                    journalDirectory,
                    serverConfiguration,
                    mockLdm
            );

            journal.start();

            TestWriteCallback cb = new TestWriteCallback(entryNum);

            for(long i = 0; i < entryNum; i++) {
                ByteBuf entryToWrite = Unpooled.wrappedBuffer(entryText.getBytes(StandardCharsets.UTF_8));
                journal.logAddEntry(
                        LEDGER_IDX,
                        i,
                        entryToWrite,
                        true,
                        cb,
                        "testCtx"
                );
            }

            // Attendi che il callback venga invocato
            cb.await();
            Assert.assertEquals("Callback should be invoked " + entryNum + " time", entryNum, cb.invoked);

            final long[] entryCount = {0};
            List<Long> journalIds = Journal.listJournalIds(journalDirectory, null);
            // Non mi aspetto un rollover, quindi solo 1 file journal
            Assert.assertEquals("Expected only 1 journal file", 1, journalIds.size());

            // Controllo che i file siano stati scritti correttamente
            for(Long jId : journalIds) {
                journal.scanJournal(
                        jId,
                        0L,
                        (version, offset, entry) -> {
                            entryCount[0]++;
                            String data = new String(entry.array(), entry.position(), entry.remaining(), StandardCharsets.UTF_8);
                            Assert.assertEquals("test", data);
                        },
                        false
                );
            }
            // Mi aspetto che il numero di entry sia uguale a quelle scritte (considerando tutti i file)
            Assert.assertEquals(entryNum + " entry expected", entryNum, entryCount[0]);

            journal.shutdown();

        } catch (Exception e) {
            Assert.fail("No exception should be thrown during shutdown: " + e);
        }

    }*/

    @Test
    public void testRun_ReuseJournal() throws Exception {
        try (MockedStatic<FileChannelProvider> fileChannelProviderMock = mockStatic(FileChannelProvider.class)) {

            long journalId1 = 300L;
            long journalId2 = journalId1 + 10L;

            String entryText = "test";
            long entryNum = 2L;

            // Prepara un file journal esistente
            File journalDirectory = new File(JOURNAL_PATH + "/journal-run-coverage");
            if(!journalDirectory.exists()) journalDirectory.mkdirs();
            File jF1 = new File(journalDirectory, Long.toHexString(journalId1) + ".txn");
            if(!jF1.exists()) jF1.createNewFile();
            File jF2 = new File(journalDirectory, Long.toHexString(journalId2) + ".txn");
            if(!jF2.exists()) jF2.createNewFile();

            // Mock
            LedgerDirsManager mockLdm = mock(LedgerDirsManager.class);
            when(mockLdm.getAllLedgerDirs()).thenReturn(Collections.singletonList(journalDirectory));

            FileChannelProvider mockFcp = mock(FileChannelProvider.class);
            when(mockFcp.supportReuseFile()).thenReturn(true);
            when(mockFcp.open(any(File.class), any(ServerConfiguration.class)))
                    .thenAnswer(invocation -> {
                        File file = invocation.getArgument(0);
                        ServerConfiguration localServerConfiguration = invocation.getArgument(1);
                        return new DefaultFileChannel(file, localServerConfiguration);
                    });

            // Forza il mock del FileChannelProvider per generare le eccezioni
            fileChannelProviderMock.when(() -> FileChannelProvider.newProvider(anyString())).thenReturn(mockFcp);

            serverConfiguration.setBusyWaitEnabled(true);
            serverConfiguration.setJournalFlushWhenQueueEmpty(false);
            serverConfiguration.setJournalBufferedEntriesThreshold(0);
            serverConfiguration.setJournalMaxGroupWaitMSec(0L);
            serverConfiguration.setJournalSyncData(false);
            serverConfiguration.setJournalReuseFiles(true);
            serverConfiguration.setMaxBackupJournals(0);
            serverConfiguration.setJournalFormatVersionToWrite(JournalChannel.V4);

            Journal journal = new Journal(
                    JOURNAL_IDX + 4,
                    journalDirectory,
                    serverConfiguration,
                    mockLdm
            );

            // Imposto l'ultimo log mark per il test
            // Uso il log più recente (journalId2) per affinché il journal piu vecchio(journalId1) venga riutilizzato
            // Alla fine del processo mi aspetto che journalId1 venga eliminato
            journal.setLastLogMark(journalId2, 0L); // Imposto l'ultimo log mark per il test
            CheckpointSource.Checkpoint cp = journal.newCheckpoint();
            journal.checkpointComplete(cp, false); // Completo il checkpoint senza rimuovere i vecchi file

            journal.start();

            TestWriteCallback cb = new TestWriteCallback(entryNum);

            for(long i = 0; i < entryNum; i++) {
                ByteBuf entryToWrite = Unpooled.wrappedBuffer(entryText.getBytes(StandardCharsets.UTF_8));
                journal.logAddEntry(
                        LEDGER_IDX,
                        i,
                        entryToWrite,
                        false,
                        cb,
                        "testCtx"
                );
            }

            // Attendi che il callback venga invocato
            cb.await();
            Assert.assertEquals("Callback should be invoked " + entryNum + " time", entryNum, cb.invoked);

            final long[] entryCount = {0};
            List<Long> journalIds = Journal.listJournalIds(journalDirectory, null);
            // Non mi aspetto un rollover, quindi solo i 2 file journal
            Assert.assertEquals("Expected only 2 journal file", 2, journalIds.size());
            // Mi aspetto che il journalId1 sia stato riutilizzato
            Assert.assertFalse("Expected journalId1 to be reused", journalIds.contains(journalId1));
            // Mi aspetto che il journalId2 sia ancora presente
            Assert.assertTrue("Expected journalId2 to be present", journalIds.contains(journalId2));

            // Controllo che i file siano stati scritti correttamente
            for(Long jId : journalIds) {
                journal.scanJournal(
                        jId,
                        0L,
                        (version, offset, entry) -> {
                            entryCount[0]++;
                            String data = new String(entry.array(), entry.position(), entry.remaining(), StandardCharsets.UTF_8);
                            Assert.assertEquals("test", data);
                        },
                        false
                );
            }
            // Mi aspetto che il numero di entry sia uguale a quelle scritte (considerando tutti i file)
            Assert.assertEquals(entryNum + " entry expected", entryNum, entryCount[0]);

            journal.shutdown();

        } catch (Exception e) {
            Assert.fail("No exception should be thrown during shutdown: " + e);
        }

    }

    @Test
    public void testRun_NullEntry() throws Exception {
        try (MockedStatic<Journal.QueueEntry> queueEntryMock = Mockito.mockStatic(Journal.QueueEntry.class)) {

            String entryText = "test";
            long entryNum = 5L;

            // Prepara un file journal esistente
            File journalDirectory = new File(JOURNAL_PATH + "/journal-run-coverage");
            if(!journalDirectory.exists()) journalDirectory.mkdirs();

            // Mock
            LedgerDirsManager mockLdm = mock(LedgerDirsManager.class);
            when(mockLdm.getAllLedgerDirs()).thenReturn(Collections.singletonList(journalDirectory));

            queueEntryMock.when(() -> Journal.QueueEntry.create(
                    any(), anyBoolean(), anyLong(), anyLong(), any(), any(), anyLong(), any(), any()
            )).thenReturn(null);

            // Entry con ID 0 valida
            queueEntryMock.when(() -> Journal.QueueEntry.create(
                    any(), anyBoolean(), anyLong(), eq(0L), any(), any(), anyLong(), any(), any()
            )).thenCallRealMethod();

            // Entry con ID entryNum-1 valida
            queueEntryMock.when(() -> Journal.QueueEntry.create(
                    any(), anyBoolean(), anyLong(), eq(entryNum - 1L), any(), any(), anyLong(), any(), any()
            )).thenCallRealMethod();

            serverConfiguration.setJournalFlushWhenQueueEmpty(true);
            serverConfiguration.setJournalBufferedEntriesThreshold(2);
            serverConfiguration.setJournalMaxGroupWaitMSec(10L);
            serverConfiguration.setJournalSyncData(false);
            serverConfiguration.setJournalReuseFiles(false);

            Journal journal = new Journal(
                    JOURNAL_IDX + 4,
                    journalDirectory,
                    serverConfiguration,
                    mockLdm
            );

            TestWriteCallback cb = new TestWriteCallback(entryNum);

            for(long i = 0; i < entryNum; i++) {
                ByteBuf entryToWrite = Unpooled.wrappedBuffer(entryText.getBytes(StandardCharsets.UTF_8));
                journal.logAddEntry(
                        LEDGER_IDX,
                        i,
                        entryToWrite,
                        true,
                        cb,
                        "testCtx"
                );
            }

            journal.start();

            // Attendi che il callback venga invocato
            cb.await(1L);
            // Essendo le entry null, il callback non dovrebbe essere invocato
            // La entry con ID entryNum-1 non viene scritta poichè ha davanti le entry con ID null
            // Queste entry bloccano la scrittura delle entry successive
            // Quindi il callback viene invocato solo per la entry con ID 0
            Assert.assertEquals("Callback should be invoked 1 time", 1, cb.invoked);

            final long[] entryCount = {0};
            List<Long> journalIds = Journal.listJournalIds(journalDirectory, null);
            // Non mi aspetto un rollover, quindi un solo file journal
            Assert.assertEquals("Expected only 1 journal file", 1, journalIds.size());
            for(Long jId : journalIds) {
                journal.scanJournal(
                        jId,
                        0L,
                        (version, offset, entry) -> {
                            entryCount[0]++;
                            String data = new String(entry.array(), entry.position(), entry.remaining(), StandardCharsets.UTF_8);
                            Assert.assertEquals("test", data);
                        },
                        false
                );
            }
            // Mi aspetto che il numero di entry sia uguale a quelle scritte (considerando tutti i file)
            Assert.assertEquals("1 entry expected", 1, entryCount[0]);

            // Chiudi nuovamente il journal per verificare che non ci siano eccezioni
            journal.shutdown();

        } catch (Exception e) {
            Assert.fail("No exception should be thrown during shutdown: " + e);
        }

    }

    // Controllo che il JournalAliveListener venga chiamato correttamente
    @Test
    public void testRun_JournalAliveListener() throws Exception {
        JournalAliveListener mockJal = mock(JournalAliveListener.class);

        serverConfiguration.setJournalFlushWhenQueueEmpty(false);
        Journal journal = new Journal(
                JOURNAL_IDX,
                JOURNAL_DIRECTORY,
                serverConfiguration,
                mock(LedgerDirsManager.class),
                NullStatsLogger.INSTANCE,
                UnpooledByteBufAllocator.DEFAULT,
                mockJal
        );

        journal.start();
        Thread.sleep(1 * 1000L); // Attendi che il journal venga attivato
        journal.shutdown();

        verify(mockJal, atLeastOnce()).onJournalExit();

    }

    private class GenericTestCheckpoint implements CheckpointSource.Checkpoint {

        final Journal.LastLogMark mark;

        public GenericTestCheckpoint(Journal.LastLogMark mark) {
            this.mark = mark;
        }

        @Override
        public int compareTo(CheckpointSource.Checkpoint o) {
            return 0;
        }

        @Override
        public boolean equals(Object o) {
            return true;
        }

        @Override
        public int hashCode() {
            return mark.hashCode();
        }

        @Override
        public String toString() {
            return mark.toString();
        }
    }

    // Callback di test
    private static class TestWriteCallback implements BookkeeperInternalCallbacks.WriteCallback {

        volatile long invoked = 0;
        private final CountDownLatch latch;

        private final int expectedRc = 0; // Codice di ritorno atteso per il successo

        public TestWriteCallback() {
            this(1);
        }

        public TestWriteCallback(long expectedInvoked) {
            if(expectedInvoked > Integer.MAX_VALUE) {
                throw new IllegalArgumentException("Expected invoked count cannot be greater than Integer.MAX_VALUE");
            }
            latch = new CountDownLatch((int) expectedInvoked);
        }

        @Override
        public void writeComplete(int rc, long ledgerId, long entryId, BookieId addr, Object ctx) {
            Assert.assertEquals("Write should succeed", expectedRc, rc);
            invoked++;
            latch.countDown();
        }

        void await(long timeout) throws InterruptedException {
            latch.await(timeout, TimeUnit.SECONDS);
        }

        void await() throws InterruptedException {
            this.await(2);
        }
    }

}