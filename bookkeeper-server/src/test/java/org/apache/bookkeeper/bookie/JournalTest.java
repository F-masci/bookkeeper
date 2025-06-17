package org.apache.bookkeeper.bookie;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.kerby.util.Hex;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

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
        Long journalId = System.currentTimeMillis();
        try {
            journal.newLogFile(journalId, null)
                    .close();
        } catch (Exception e) {
            Assert.fail("Failed to create dummy journal file: " + e.getMessage());
        }
        return journalId;
    }

    private void writeToDummyJournalFile() throws IOException {
        File dummyJournalFile = new File(JOURNAL_DIRECTORY, Long.toHexString(dummyJournalId) + ".txn");
        FileChannel fc = new RandomAccessFile(dummyJournalFile, "rw").getChannel();
        ByteBuffer buf = ByteBuffer.wrap("Dummy Journal Entry".getBytes());
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
    public void setUp() {

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
    public void testValidDirectoryRetrivialJournalIds() {

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

    }

    @Test
    public void testJournalWrite() {

        final long entryId = 1L;
        ByteBuf buf = Unpooled.wrappedBuffer(new byte[]{1, 2, 3, 4, 5});

        // Test writing to the journal
        try {
            journal.logAddEntry(
                LEDGER_IDX,
                entryId,
                buf,
                false,
                null,
                null
            );
        } catch (Exception e) {
            Assert.fail("Expected no exception during journal write: " + e.getMessage());
        }
    }

    @Test
    public void testDummyJournalRead() {

        // Test reading from the journal
        try {

            // Write to the dummy journal file
            writeToDummyJournalFile();

            journal.scanJournal(
                    dummyJournalId,
                    0L,
                    new Journal.JournalScanner() {
                        @Override
                        public void process(int journalVersion, long offset, ByteBuffer entry) throws IOException {
                            Assert.fail("Expected no processing of journal entries in this test");
                        }
                    },
                    true
            );
        } catch (Exception e) {
            Assert.fail("Expected no exception during journal read: " + e.getMessage());
        }
    }

}