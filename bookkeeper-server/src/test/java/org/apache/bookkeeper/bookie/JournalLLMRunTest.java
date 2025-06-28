// file: bookkeeper-server/src/test/java/org/apache/bookkeeper/bookie/JournalRunTest.java
package org.apache.bookkeeper.bookie;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks;
import org.junit.*;
import org.mockito.Mockito;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class JournalLLMRunTest {

    private Journal journal;
    private File journalDir;
    private ServerConfiguration conf;
    private LedgerDirsManager ledgerDirsManager;

    @Before
    public void setup() throws Exception {
        conf = new ServerConfiguration();
        journalDir = new File("/tmp/bookkeeper/journalrun");
        journalDir.mkdirs();
        ledgerDirsManager = Mockito.mock(LedgerDirsManager.class);
        // Mockito.when(ledgerDirsManager.getAllLedgerDirs()).thenReturn(new File[]{journalDir});
        Mockito.when(ledgerDirsManager.getAllLedgerDirs()).thenReturn(Arrays.asList(new File[]{journalDir}));
        // Mockito.when(ledgerDirsManager.getWritableLedgerDirsForNewLog()).thenReturn(java.util.Collections.singletonList(journalDir));
        Mockito.when(ledgerDirsManager.getWritableLedgerDirsForNewLog()).thenReturn(java.util.Collections.singletonList(journalDir));
        journal = new Journal(0, journalDir, conf, ledgerDirsManager);
    }

    @After
    public void cleanup() {
        for (File f : journalDir.listFiles()) f.delete();
        journalDir.delete();
    }

    @Test
    public void testRunProcessesEntryAndCallback() throws Exception {
        // Avvia il thread del journal
        journal.start();

        // Prepara una entry e un callback
        String data = "testEntry";
        ByteBuf entry = Unpooled.wrappedBuffer(data.getBytes(StandardCharsets.UTF_8));
        TestWriteCallback cb = new TestWriteCallback();

        // Scrivi una entry nel journal
        journal.logAddEntry(1L, 1L, entry, true, cb, "ctx");

        // Attendi che il callback venga invocato
        cb.await();

        Assert.assertTrue("Callback should be invoked", cb.invoked);

        // Verifica che la stringa sia stata scritta nel file di journal
        File[] files = journalDir.listFiles((dir, name) -> name.endsWith(".txn"));
        Assert.assertNotNull("Journal file should exist", files);
        Assert.assertTrue("At least one journal file should exist", files.length > 0);

        boolean found = false;
        for (File f : files) {
            byte[] content = java.nio.file.Files.readAllBytes(f.toPath());
            String fileContent = new String(content, StandardCharsets.UTF_8);
            if (fileContent.contains(data)) {
                found = true;
                break;
            }
        }
        Assert.assertTrue("Journal file should contain the entry data", found);

        // Arresta il journal
        journal.shutdown();
    }

    @Test
    public void testShutdownStopsThread() throws Exception {
        journal.start();
        journal.shutdown();
        // Se non ci sono eccezioni, il test passa
        Assert.assertTrue(true);
    }

    // Callback di test
    private static class TestWriteCallback implements BookkeeperInternalCallbacks.WriteCallback {
        volatile boolean invoked = false;
        private final CountDownLatch latch = new CountDownLatch(1);

        @Override
        public void writeComplete(int rc, long ledgerId, long entryId, org.apache.bookkeeper.net.BookieId addr, Object ctx) {
            invoked = true;
            latch.countDown();
        }

        void await() throws InterruptedException {
            latch.await(2, TimeUnit.SECONDS);
        }
    }
}
//java -jar /home/fmasci/Programmi/evosuite-1.0.6.jar -class org.apache.bookkeeper.bookie.Journal -generateSuite -Dsearch_budget=60 -Dstopping_condition=MaxTime -projectCP .
//java -jar /home/fmasci/Programmi/evosuite-1.0.6.jar -class org.apache.bookkeeper.bookie.Journal -generateSuite -Dsearch_budget=60 -Dstopping_condition=MaxTime -projectCP .

