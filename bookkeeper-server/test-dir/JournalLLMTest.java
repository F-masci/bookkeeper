package org.apache.bookkeeper.bookie;

import io.netty.buffer.Unpooled;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.junit.*;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class JournalLLMTest {

    private File tempDir;
    private LedgerDirsManager ledgerDirsManager;
    private ServerConfiguration conf;
    private Journal journal;

    @Before
    public void setUp() throws Exception {
        tempDir = new File(System.getProperty("java.io.tmpdir"), "bk-journal-test-" + System.nanoTime());
        tempDir.mkdirs();
        ledgerDirsManager = mock(LedgerDirsManager.class);
        // when(ledgerDirsManager.getAllLedgerDirs()).thenReturn(new File[]{tempDir});
        when(ledgerDirsManager.getAllLedgerDirs()).thenReturn(Arrays.asList(new File[]{tempDir}));
        // when(ledgerDirsManager.getWritableLedgerDirsForNewLog()).thenReturn(List.of(tempDir));
        when(ledgerDirsManager.getWritableLedgerDirsForNewLog()).thenReturn(Arrays.asList(tempDir));
        conf = new ServerConfiguration();
        conf.setJournalDirName(tempDir.getAbsolutePath());
        // conf.setJournalDirs(new String[]{tempDir.getAbsolutePath()});
        conf.setJournalQueueSize(10);
        conf.setMaxJournalSizeMB(1);
        // conf.setJournalPreAllocSizeMB(1);
        conf.setJournalWriteBufferSizeKB(4);
        conf.setMaxBackupJournals(2);
        conf.setJournalSyncData(true);
        conf.setJournalFormatVersionToWrite(5);
        conf.setJournalAlignmentSize(512);
        conf.setJournalPageCacheFlushIntervalMSec(1000);
        conf.setJournalReuseFiles(false);
        conf.setJournalMaxMemorySizeMb(1);
        conf.setJournalBufferedEntriesThreshold(1);
        conf.setJournalBufferedWritesThreshold(1);
        conf.setJournalMaxGroupWaitMSec(10);
        conf.setJournalFlushWhenQueueEmpty(true);
        conf.setJournalRemovePagesFromCache(false);
        conf.setBookiePort(3181);

        journal = new Journal(0, tempDir, conf, ledgerDirsManager, NullStatsLogger.INSTANCE, Unpooled.buffer().alloc());
    }

    @After
    public void tearDown() {
        for (File f : tempDir.listFiles()) {
            f.delete();
        }
        tempDir.delete();
    }

    // --- TEST: listJournalIds ---

    @Test
    public void testListJournalIds_EmptyDir() {
        // Verifica che una directory vuota restituisca una lista vuota
        List<Long> ids = Journal.listJournalIds(tempDir, null);
        assertTrue(ids.isEmpty());
    }

    @Test
    public void testListJournalIds_InvalidFiles() throws IOException {
        // Crea file con estensione errata
        File invalid = new File(tempDir, "notajournal.txt");
        assertTrue(invalid.createNewFile());
        List<Long> ids = Journal.listJournalIds(tempDir, null);
        assertTrue(ids.isEmpty());
    }

    @Test
    public void testListJournalIds_ValidAndInvalidFiles() throws IOException {
        // Crea file valido e uno non valido
        long id = System.currentTimeMillis();
        File valid = new File(tempDir, Long.toHexString(id) + ".txn");
        assertTrue(valid.createNewFile());
        File invalid = new File(tempDir, "dummy.txt");
        assertTrue(invalid.createNewFile());
        List<Long> ids = Journal.listJournalIds(tempDir, null);
        assertEquals(1, ids.size());
        assertEquals(id, (long) ids.get(0));
    }

    @Test
    public void testListJournalIds_WithFilter() throws IOException {
        long id1 = 0x1A;
        long id2 = 0x2B;
        new File(tempDir, Long.toHexString(id1) + ".txn").createNewFile();
        new File(tempDir, Long.toHexString(id2) + ".txn").createNewFile();
        List<Long> ids = Journal.listJournalIds(tempDir, journalId -> journalId == id2);
        assertEquals(1, ids.size());
        assertEquals(id2, (long) ids.get(0));
    }

    // --- TEST: scanJournal ---

    @Test
    public void testScanJournal_EmptyFile() throws IOException {
        long id = System.currentTimeMillis();
        File f = new File(tempDir, Long.toHexString(id) + ".txn");
        assertTrue(f.createNewFile());
        long pos = journal.scanJournal(id, 0, (ver, offset, entry) -> fail("Non deve essere chiamato"), true);
        assertEquals(f.length(), pos);
    }

    @Test
    public void testScanJournal_InvalidId() throws IOException {
        // Prova a leggere un file che non esiste
        long id = 123456789L;
        try {
            journal.scanJournal(id, 0, (ver, offset, entry) -> fail("Non deve essere chiamato"), true);
        } catch (IOException e) {
            // Atteso: il file non esiste
        }
    }

    @Test
    public void testScanJournal_ValidEntry() throws Exception {
        // Scrive un entry e la legge
        long id = System.currentTimeMillis();
        File f = new File(tempDir, Long.toHexString(id) + ".txn");
        try (java.io.RandomAccessFile raf = new java.io.RandomAccessFile(f, "rw")) {
            byte[] data = "test".getBytes(StandardCharsets.UTF_8);
            raf.writeInt(data.length);
            raf.write(data);
        }
        final boolean[] called = {false};
        long pos = journal.scanJournal(id, 0, (ver, offset, entry) -> {
            called[0] = true;
            byte[] arr = new byte[entry.remaining()];
            entry.get(arr);
            assertArrayEquals("test".getBytes(StandardCharsets.UTF_8), arr);
        }, false);
        assertTrue(called[0]);
        assertEquals(f.length(), pos);
    }

    // --- TEST: logAddEntry ---

    /*@Test
    public void testLogAddEntry_Valid() {
        try {
            ByteBuf entry = Unpooled.buffer();
            entry.writeLong(1L); // ledgerId
            entry.writeLong(2L); // entryId
            entry.writeBytes("data".getBytes(StandardCharsets.UTF_8));
            WriteCallback cb = mock(WriteCallback.class);
            journal.logAddEntry(1L, 2L, entry, true, cb, "ctx");
            // Non possiamo verificare la scrittura effettiva senza avviare il thread, ma il test verifica che non lanci eccezioni
        } catch (Exception e) {
            fail("Non dovrebbe lanciare eccezioni: " + e.getMessage());
        }

    }*/

    /*@Test
    public void testLogAddEntry_NegativeIds() {
        try {
            ByteBuf entry = Unpooled.buffer();
            entry.writeLong(-1L);
            entry.writeLong(-2L);
            entry.writeBytes("data".getBytes(StandardCharsets.UTF_8));
            WriteCallback cb = mock(WriteCallback.class);
            journal.logAddEntry(-1L, -2L, entry, false, cb, null);
        } catch (Exception e) {
            fail("Non dovrebbe lanciare eccezioni: " + e.getMessage());
        }
    }*/

    /*@Test(expected = NullPointerException.class)
    public void testLogAddEntry_NullEntry() throws Exception {
        journal.logAddEntry(1L, 2L, null, true, mock(WriteCallback.class), null);
    }*/

    // --- TEST: getJournalQueueLength ---

    @Test
    public void testGetJournalQueueLength_Empty() {
        assertEquals(0, journal.getJournalQueueLength());
    }

    /*@Test
    public void testGetJournalQueueLength_AfterAdd() throws Exception {
        ByteBuf entry = Unpooled.buffer();
        entry.writeLong(1L);
        entry.writeLong(2L);
        entry.writeBytes("data".getBytes(StandardCharsets.UTF_8));
        journal.logAddEntry(1L, 2L, entry, true, mock(WriteCallback.class), null);
        assertEquals(1, journal.getJournalQueueLength());
    }*/

    // --- TEST: setLastLogMark e getLastLogMark ---

    @Test
    public void testSetAndGetLastLogMark() {
        journal.setLastLogMark(123L, 456L);
        assertEquals(123L, journal.getLastLogMark().getCurMark().getLogFileId());
        assertEquals(456L, journal.getLastLogMark().getCurMark().getLogFileOffset());
    }

    // --- TEST: newCheckpoint e checkpointComplete ---

    @Test
    public void testNewCheckpointAndComplete() throws IOException {
        Journal.Checkpoint cp = journal.newCheckpoint();
        // Non dovrebbe lanciare eccezioni
        journal.checkpointComplete(cp, false);
    }

    @Test
    public void testCheckpointCompleteWithCompact() throws IOException {
        Journal.Checkpoint cp = journal.newCheckpoint();
        // Simula la presenza di vecchi file di journal
        for (int i = 0; i < 5; i++) {
            new File(tempDir, Long.toHexString(System.currentTimeMillis() + i) + ".txn").createNewFile();
        }
        journal.checkpointComplete(cp, true);
        // Dovrebbe eliminare i file più vecchi se superano maxBackupJournals
    }

    // --- TEST: getJournalDirectory ---

    @Test
    public void testGetJournalDirectory() {
        assertEquals(tempDir, journal.getJournalDirectory());
    }

    // --- TEST: shutdown ---

    @Test
    public void testShutdown() {
        try {
            journal.shutdown();
        } catch (Exception e) {
            fail("Non dovrebbe lanciare eccezioni durante lo shutdown: " + e.getMessage());
        }
    }
}