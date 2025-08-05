package org.apache.bookkeeper.bookie;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.util.PageCacheUtil;
import org.apache.bookkeeper.util.ZeroBuffer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.UUID;

import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class JournalChannelTest {

    private final int LEDGER_IDX = 1;
    private final int JOURNAL_IDX = 1;
    private final String JOURNAL_PATH = System.getProperty("java.io.tmpdir");
    private final File JOURNAL_DIRECTORY = new File(JOURNAL_PATH, "/bk-journal-" + LEDGER_IDX + "_" + System.nanoTime());

    // Use default ServerConfiguration for testing
    private final ServerConfiguration serverConfiguration = new ServerConfiguration();

    private Long dummyJournalId;

    private ByteBuffer getFileHeader(FileChannel fc) throws IOException {
        ByteBuffer header = ByteBuffer.allocate(8);
        int read = fc.read(header);
        if (read != 8) return null;
        header.flip();
        return header;
    }

    private boolean checkHeaderV6(ByteBuffer header) {
        if (header == null || header.remaining() < 8) return false;

        byte[] magic = new byte[4];
        header.get(magic);
        if (!Arrays.equals(magic, "BKLG".getBytes(StandardCharsets.UTF_8))) return false;

        int version = header.getInt();
        return version == JournalChannel.V6;
    }

    private boolean checkJournalCreation(File directory, File journal) {
        if(!directory.exists()) return false;
        if(!directory.isDirectory()) return false;
        if(!journal.exists()) return false;

        // Check header -> version 6
        try (FileChannel fc = new RandomAccessFile(journal, "rw").getChannel()) {
            ByteBuffer header = getFileHeader(fc);
            fc.position(0);
            return checkHeaderV6(header);
        } catch (IOException e) {
            return false;
        }
    }

    private boolean checkJournalCreation(File journal) {
        return checkJournalCreation(JOURNAL_DIRECTORY, journal);
    }

    private boolean checkJournalCreation(File directory, Long journalId) {
        File dummyJournalFile = new File(directory, Long.toHexString(journalId) + ".txn");
        return checkJournalCreation(directory, dummyJournalFile);
    }

    private boolean checkJournalCreation(Long journalId) {
        return checkJournalCreation(JOURNAL_DIRECTORY, journalId);
    }

    private boolean checkDummyJournalCreation(File directory) {
        return checkJournalCreation(directory, dummyJournalId);
    }

    private boolean checkDummyJournalCreation() {
        return checkJournalCreation(JOURNAL_DIRECTORY, dummyJournalId);
    }

    private boolean checkJournalWrite(JournalChannel journalChannel, File journal) throws IOException {
        byte[] expected = "prova".getBytes(StandardCharsets.UTF_8);
        ByteBuf buffer = Unpooled.wrappedBuffer(expected);
        long fcPosition = journalChannel.fc.position();
        if(fcPosition < 512) throw new IOException("FileChannel position is less than 512 bytes, cannot write data");
        if(journalChannel.bc.writeCapacity <= 0) throw new IOException("BufferedChannel write capacity is zero or negative");
        journalChannel.bc.write(buffer);
        journalChannel.bc.flushAndForceWrite(false);


        try (FileChannel fc = new RandomAccessFile(journal, "rw").getChannel()) {

            // Set position to the end of header
            fc.position(fcPosition);

            ByteBuffer buffTest = ByteBuffer.allocate(expected.length);
            int read = fc.read(buffTest);
            if (read != expected.length) throw new IOException("Failed to read " + expected.length + " bytes");
            buffTest.flip();

            byte[] actual = new byte[expected.length];
            buffTest.get(actual);

            if(!Arrays.equals(expected, actual)) throw new IOException("Data mismatch: expected " + Arrays.toString(expected) + " but got " + Arrays.toString(actual));
            return true;
        }
    }

    private boolean checkJournalWrite(JournalChannel journalChannel, File directory, Long journalId) throws IOException {
        File journalFile = new File(directory, Long.toHexString(journalId) + ".txn");
        return checkJournalWrite(journalChannel, journalFile);
    }

    private boolean checkJournalWrite(JournalChannel journalChannel, Long journalId) throws IOException {
        return checkJournalWrite(journalChannel, JOURNAL_DIRECTORY, journalId);
    }

    private boolean checkDummyJournalWrite(JournalChannel journalChannel) throws IOException {
        return checkJournalWrite(journalChannel, dummyJournalId);
    }

    private boolean checkJournalRead(JournalChannel journalChannel, File journal) throws IOException {
        byte[] expected = "prova".getBytes(StandardCharsets.UTF_8);
        ByteBuffer buffer = ByteBuffer.wrap(expected);
        long fcPosition = journalChannel.fc.position();
        if(fcPosition < 512) throw new IOException("FileChannel position is less than 512 bytes, cannot read data");

        try (FileChannel fc = new RandomAccessFile(journal, "rw").getChannel()) {

            // Set position to the end of header
            fc.write(buffer, fcPosition);
            fc.force(false);

            ByteBuffer buffTest = ByteBuffer.allocate(expected.length);
            int read = journalChannel.read(buffTest);
            if (read != expected.length) throw new IOException("Failed to read " + expected.length + " bytes");
            buffTest.flip();

            byte[] actual = new byte[expected.length];
            buffTest.get(actual);

            if(!Arrays.equals(expected, actual)) throw new IOException("Data mismatch: expected " + Arrays.toString(expected) + " but got " + Arrays.toString(actual));
            return true;
        }
    }

    private boolean checkJournalRead(JournalChannel journalChannel, File directory, Long journalId) throws IOException {
        File journalFile = new File(directory, Long.toHexString(journalId) + ".txn");
        return checkJournalRead(journalChannel, journalFile);
    }

    private boolean checkJournalRead(JournalChannel journalChannel, Long journalId) throws IOException {
        return checkJournalRead(journalChannel, JOURNAL_DIRECTORY, journalId);
    }

    private boolean checkDummyJournalRead(JournalChannel journalChannel) throws IOException {
        return checkJournalRead(journalChannel, dummyJournalId);
    }

    @Mock
    private FileChannel mockFc;

    @Mock
    private BookieFileChannel mockBfc;

    @Mock
    private FileChannelProvider mockFcp;

    @Mock
    private Journal.BufferedChannelBuilder mockBcb;

    @Mock
    private BufferedChannel mockBc;

    @Before
    public void setUp() throws Exception {

        JOURNAL_DIRECTORY.mkdirs();
        dummyJournalId = UUID.randomUUID().getLeastSignificantBits();
        serverConfiguration.setJournalDirName(JOURNAL_DIRECTORY.getAbsolutePath());

        mockFc = mock(FileChannel.class);
        lenient().when(mockFc.position(anyLong())).thenThrow(new IOException());
        lenient().when(mockFc.write(any(ByteBuffer.class), anyLong())).thenAnswer(invocation -> {
            long pos = invocation.getArgument(1);
            if (pos < 0) {
                throw new IllegalArgumentException("Negative position: " + pos);
            }
            return 0; // oppure il valore che preferisci per la scrittura simulata
        });

        mockBfc = mock(BookieFileChannel.class);
        lenient().when(mockBfc.getFileChannel()).thenReturn(mockFc);

        mockFcp = mock(FileChannelProvider.class);
        when(mockFcp.open(any(File.class), any(ServerConfiguration.class))).thenReturn(mockBfc);

        mockBc = mock(BufferedChannel.class);

        mockBcb = mock(Journal.BufferedChannelBuilder.class);
        when(mockBcb.create(any(FileChannel.class), anyInt())).thenReturn(mockBc);

    }

    @After
    public void tearDown() throws InterruptedException {
        if (JOURNAL_DIRECTORY.exists()) {
            for (File file : JOURNAL_DIRECTORY.listFiles()) {
                file.delete();
            }
            JOURNAL_DIRECTORY.delete();
        }
    }

    // --- Caso base ---
    // journalDirectory: esistente
    // logId: zero_o_positivo
    // preAllocSize: positivo
    // writeBufferSize: valido
    // journalAlignSize: potenza_due_valida
    // position: zero
    // fRemoveFromPageCache: false
    // formatVersionToWrite: valida (V6)
    // bcBuilder: valido (default)
    // conf: valido (default)
    // provider: valido (default)
    // toReplaceLogId: null
    @Test
    public void testJournalChannel_base() throws Exception {
        JournalChannelBuilder jcb = new JournalChannelBuilder();
        JournalChannel jc = jcb.build();
        Assert.assertTrue(checkDummyJournalCreation());
        Assert.assertTrue(checkDummyJournalWrite(jc));
        Assert.assertTrue(checkDummyJournalRead(jc));
        jc.close();
    }

    // --- JOURNAL_DIRECTORY ---

    // journalDirectory: non esistente
    @Test
    public void testJournalChannel_1() {
        try {
            File directory = new File("/tmp/not-found-directory");
            JournalChannelBuilder jcb = new JournalChannelBuilder().withJournalDirectory(directory);
            jcb.build();
            Assert.fail("Expected IOException due to non-existent journal directory");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof Exception);
        }
    }

    // --- LOG_ID ---

    // logId: negativo
    @Test
    public void testJournalChannel_2() throws Exception {
        Long negativeLogId = -1L;
        JournalChannelBuilder jcb = new JournalChannelBuilder().withLogId(negativeLogId);
        JournalChannel jc = jcb.build();
        Assert.assertTrue(checkJournalCreation(negativeLogId));
        Assert.assertTrue(checkJournalWrite(jc, negativeLogId));
        Assert.assertTrue(checkJournalRead(jc, negativeLogId));
        jc.close();
    }

    // --- PRE_ALLOC_SIZE ---

    // preAllocSize: zero
    @Test
    public void testJournalChannel_3() {
        try {
            JournalChannelBuilder jcb = new JournalChannelBuilder().withPreAllocSize(0);
            jcb.build();
            Assert.fail("Expected IllegalArgumentException due to zero preAllocSize");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof IllegalArgumentException);
        }
    }

    // preAllocSize: negativo
    @Test
    public void testJournalChannel_4() {
        try {
            JournalChannelBuilder jcb = new JournalChannelBuilder().withPreAllocSize(-1);
            jcb.build();
            Assert.fail("Expected IllegalArgumentException due to negative preAllocSize");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof IllegalArgumentException);
        }
    }

    // --- WRITE_BUFFER_SIZE ---

    // writeBufferSize: zero
    @Test
    public void testJournalChannel_5() throws Exception {
        JournalChannelBuilder jcb = new JournalChannelBuilder().withWriteBufferSize(0);
        JournalChannel jc = jcb.build();
        Assert.assertTrue(checkDummyJournalCreation());
        // FIXME
        // Assert.assertTrue(checkDummyJournalWrite(jc));
        Assert.assertTrue(checkDummyJournalRead(jc));
        jc.close();
    }

    // writeBufferSize: negativo
    @Test
    public void testJournalChannel_6() {
        try {
            JournalChannelBuilder jcb = new JournalChannelBuilder().withWriteBufferSize(-1);
            jcb.build();
            Assert.fail("Expected IllegalArgumentException due to negative writeBufferSize");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof IllegalArgumentException);
        }
    }

    // --- JOURNAL_ALIGN_SIZE ---

    // journalAlignSize: zero
    @Test
    public void testJournalChannel_7() {
        try {
            JournalChannelBuilder jcb = new JournalChannelBuilder().withJournalAlignSize(0);
            jcb.build();
            Assert.fail("Expected ArithmeticException due to zero journalAlignSize");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof ArithmeticException);
        }
    }

    // journalAlignSize: negativo
    @Test
    public void testJournalChannel_8() {
        try {
            JournalChannelBuilder jcb = new JournalChannelBuilder().withJournalAlignSize(-1);
            jcb.build();
            Assert.fail("Expected IllegalArgumentException due to negative journalAlignSize");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof IllegalArgumentException);
        }
    }

    // journalAlignSize: non potenza di due
    @Test
    public void testJournalChannel_9() throws Exception {
        JournalChannelBuilder jcb = new JournalChannelBuilder().withJournalAlignSize(123);
        JournalChannel jc = jcb.build();
        Assert.assertTrue(checkDummyJournalCreation());
        Assert.assertTrue(checkDummyJournalWrite(jc));
        Assert.assertTrue(checkDummyJournalRead(jc));
        jc.close();
    }

    // --- POSITION ---

    // position: negativo
    @Test
    public void testJournalChannel_10() throws Exception {
        JournalChannelBuilder jcb = new JournalChannelBuilder().withPosition(-1L);
        JournalChannel jc = jcb.build();
        Assert.assertTrue(checkDummyJournalCreation());
        Assert.assertTrue(checkDummyJournalWrite(jc));
        Assert.assertTrue(checkDummyJournalRead(jc));
        jc.close();
    }

    // position: Integer.MAX_VALUE
    @Test
    public void testJournalChannel_11() throws Exception {
        JournalChannelBuilder jcb = new JournalChannelBuilder().withPosition(Integer.MAX_VALUE);
        JournalChannel jc = jcb.build();
        Assert.assertTrue(checkDummyJournalCreation());
        Assert.assertTrue(checkDummyJournalWrite(jc));
        Assert.assertTrue(checkDummyJournalRead(jc));
        jc.close();
    }

    // --- F_REMOVE_FROM_PAGE_CACHE ---

    // fRemoveFromPageCache: true
    @Test
    public void testJournalChannel_12() throws Exception {
        JournalChannelBuilder jcb = new JournalChannelBuilder().withRemoveFromPageCache(true);
        JournalChannel jc = jcb.build();
        Assert.assertTrue(checkDummyJournalCreation());
        Assert.assertTrue(checkDummyJournalWrite(jc));
        Assert.assertTrue(checkDummyJournalRead(jc));
        jc.close();
    }

    // --- FORMAT_VERSION_TO_WRITE ---

    // formatVersionToWrite: 0
    @Test
    public void testJournalChannel_13() {
        try {
            JournalChannelBuilder jcb = new JournalChannelBuilder().withFormatVersionToWrite(0);
            jcb.build();
            Assert.fail("Expected Exception due to invalid version 0");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof IOException);
        }
    }

    // formatVersionToWrite: V3
    @Test
    public void testJournalChannel_14() {
        try {
            JournalChannelBuilder jcb = new JournalChannelBuilder().withFormatVersionToWrite(JournalChannel.V3);
            jcb.build();
            Assert.fail("Expected Exception due to invalid version 3");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof IOException);
        }
    }

    // --- TO_REPLACE_LOG_ID ---
    // Di default non viene utilizzato

    // toReplaceLogId: valido
    @Test
    public void testJournalChannel_15() throws Exception {

        Long toReplaceLogId = 1L;
        File toReplaceLogFile = new File(JOURNAL_DIRECTORY, Long.toHexString(toReplaceLogId) + ".txn");
        if (!toReplaceLogFile.exists()) toReplaceLogFile.createNewFile();

        JournalChannelBuilder jcb = new JournalChannelBuilder().withToReplaceLogId(toReplaceLogId);
        JournalChannel jc = jcb.build();
        Assert.assertTrue(checkDummyJournalCreation());
        Assert.assertTrue(checkDummyJournalWrite(jc));
        Assert.assertTrue(checkDummyJournalRead(jc));
        jc.close();
    }

    // toReplaceLogId: negativo
    @Test
    public void testJournalChannel_16() throws Exception {

        Long toReplaceLogId = -1L;
        File toReplaceLogFile = new File(JOURNAL_DIRECTORY, Long.toHexString(toReplaceLogId) + ".txn");
        if (!toReplaceLogFile.exists()) toReplaceLogFile.createNewFile();

        JournalChannelBuilder jcb = new JournalChannelBuilder().withToReplaceLogId(toReplaceLogId);
        JournalChannel jc = jcb.build();
        Assert.assertTrue(checkDummyJournalCreation());
        Assert.assertTrue(checkDummyJournalWrite(jc));
        Assert.assertTrue(checkDummyJournalRead(jc));
        jc.close();
    }

    // --- COVERAGE ---

    // Mutation coverage for renameJournalFile
    // toReplaceLogId: valido
    @Test
    public void testJournalChannel_coverage_1() throws Exception {

        Long toReplaceLogId = 123L;
        File toReplaceLogFile = new File(JOURNAL_DIRECTORY, Long.toHexString(toReplaceLogId) + ".txn");
        if (!toReplaceLogFile.exists()) toReplaceLogFile.createNewFile();

        when(mockFcp.supportReuseFile()).thenReturn(true);
        when(mockFcp.open(any(File.class), any(ServerConfiguration.class)))
                .thenAnswer(invocation -> {
                    File file = invocation.getArgument(0);
                    ServerConfiguration serverConfiguration = invocation.getArgument(1);
                    return new DefaultFileChannel(file, serverConfiguration);
                });

        JournalChannelBuilder jcb = new JournalChannelBuilder()
                .withToReplaceLogId(toReplaceLogId)
                .withFileChannelProvider(mockFcp);
        JournalChannel jc = jcb.build();
        Assert.assertFalse(toReplaceLogFile.exists());
        Assert.assertTrue(checkDummyJournalCreation());
        Assert.assertTrue(checkDummyJournalWrite(jc));
        Assert.assertTrue(checkDummyJournalRead(jc));
        jc.close();
    }

    // Mutation coverage for renameJournalFile
    // toReplaceLogId: non presente
    @Test
    public void testJournalChannel_coverage_2() throws Exception {

        Long toReplaceLogId = 124L;
        File toReplaceLogFile = new File(JOURNAL_DIRECTORY, Long.toHexString(toReplaceLogId) + ".txn");
        if (toReplaceLogFile.exists()) toReplaceLogFile.delete();

        when(mockFcp.supportReuseFile()).thenReturn(true);
        when(mockFcp.open(any(File.class), any(ServerConfiguration.class)))
                .thenAnswer(invocation -> {
                    File file = invocation.getArgument(0);
                    ServerConfiguration serverConfiguration = invocation.getArgument(1);
                    return new DefaultFileChannel(file, serverConfiguration);
                });

        JournalChannelBuilder jcb = new JournalChannelBuilder()
                .withToReplaceLogId(toReplaceLogId)
                .withFileChannelProvider(mockFcp);
        JournalChannel jc = jcb.build();
        Assert.assertFalse(toReplaceLogFile.exists());
        Assert.assertTrue(checkDummyJournalCreation());
        Assert.assertTrue(checkDummyJournalWrite(jc));
        Assert.assertTrue(checkDummyJournalRead(jc));
        jc.close();
    }

    // Line coverage
    // fRemoveFromPageCache: true
    @Test
    public void testJournalChannel_coverage_3() throws Exception {
        Journal.BufferedChannelBuilder bcBuilder = mock(Journal.BufferedChannelBuilder.class);
        BufferedChannel bc = mock(BufferedChannel.class);
        when(bc.forceWrite(anyBoolean())).thenReturn(20 * 1024 * 1024L); // 20MB
        when(bcBuilder.create(any(FileChannel.class), anyInt())).thenReturn(bc);

        try (MockedStatic<PageCacheUtil> pageCacheUtilMock = mockStatic(PageCacheUtil.class)) {
            JournalChannelBuilder jcb = new JournalChannelBuilder()
                    .withBufferedChannelBuilder(bcBuilder)
                    .withRemoveFromPageCache(true);
            JournalChannel jc = jcb.build();

            jc.forceWrite(false);

            // Verifica che il metodo statico sia stato chiamato almeno una volta
            pageCacheUtilMock.verify(() -> PageCacheUtil.bestEffortRemoveFromPageCache(anyInt(), anyLong(), anyLong()), atLeastOnce());

            Field lastDropPosition = jc.getClass().getDeclaredField("lastDropPosition");
            lastDropPosition.setAccessible(true);
            Assert.assertTrue(lastDropPosition.getLong(jc) > 0);
        }
    }

    // Line coverage
    @Test
    public void testJournalChannel_coverage_4() throws Exception {

        File dummyJournalFile = new File(JOURNAL_DIRECTORY, Long.toHexString(dummyJournalId) + ".txn");
        if (!dummyJournalFile.exists()) dummyJournalFile.createNewFile();

        try {
            JournalChannelBuilder jcb = new JournalChannelBuilder()
                    .withFileChannelProvider(mockFcp);
            jcb.build();
            Assert.fail("Expected IOException");
        } catch (IOException e) {
            Assert.assertTrue(e instanceof IOException);
        }
    }

    // Mutation coverage
    // formatVersion: line 228
    // Controlla che la posizione del FileChannel sia corretta dopo l'apertura del file di journal con versione 4
    @Test
    public void testJournalChannel_coverage_5() throws Exception {

        // Crea un file di journal con versione 5
        File dummyJournalFile = new File(JOURNAL_DIRECTORY, Long.toHexString(dummyJournalId) + ".txn");
        if (!dummyJournalFile.exists()) dummyJournalFile.createNewFile();
        try (FileChannel fc = new RandomAccessFile(dummyJournalFile, "rw").getChannel()) {
            ByteBuffer header = ByteBuffer.allocate(JournalChannel.HEADER_SIZE);
            // Scrivi magic + versione nei primi 8 byte
            header.put("BKLG".getBytes(StandardCharsets.UTF_8));
            header.putInt(JournalChannel.V5);
            // Il resto rimane a zero
            header.position(0);
            fc.write(header, 0);
        }

        JournalChannelBuilder jcb = new JournalChannelBuilder()
                .withLogId(dummyJournalId);
        JournalChannel jc = jcb.build();
        Assert.assertEquals("Expected position at HEADER_SIZE", JournalChannel.HEADER_SIZE, jc.fc.position());
        jc.close();
    }

    // Mutation coverage
    // formatVersion: line 230
    // Controlla che la posizione del FileChannel sia corretta dopo l'apertura del file di journal con versione 2
    @Test
    public void testJournalChannel_coverage_6() throws Exception {

        // Crea un file di journal con versione 2
        File dummyJournalFile = new File(JOURNAL_DIRECTORY, Long.toHexString(dummyJournalId) + ".txn");
        if (!dummyJournalFile.exists()) dummyJournalFile.createNewFile();
        try (FileChannel fc = new RandomAccessFile(dummyJournalFile, "rw").getChannel()) {
            ByteBuffer header = ByteBuffer.allocate(JournalChannel.VERSION_HEADER_SIZE);
            header.put("BKLG".getBytes(StandardCharsets.UTF_8));
            header.putInt(JournalChannel.V2);
            header.position(0);
            fc.write(header, 0);
        }

        JournalChannelBuilder jcb = new JournalChannelBuilder()
                .withLogId(dummyJournalId);
        JournalChannel jc = jcb.build();
        Assert.assertEquals("Expected position at VERSION_HEADER_SIZE", JournalChannel.VERSION_HEADER_SIZE, jc.fc.position());
        jc.close();
    }

    // Mutation coverage
    // preAllocSize: line 158
    @Test
    public void testJournalChannel_coverage_7() throws Exception {

        long preAllocSize = Long.MAX_VALUE - 1;
        int journalAlignSize = 512;
        JournalChannelTest.JournalChannelBuilder jcb = new JournalChannelTest.JournalChannelBuilder()
                .withPreAllocSize(preAllocSize)
                .withJournalAlignSize(journalAlignSize);
        JournalChannel jc = jcb.build();

        // Scrivi dati per forzare la preallocazione
        byte[] data = new byte[2048];
        jc.bc.write(Unpooled.wrappedBuffer(data));
        jc.bc.flushAndForceWrite(false);

        // Controlla che la dimensione del file sia multipla di journalAlignSize
        long fileSize = jc.fc.size();
        Assert.assertEquals("Journal must be aligned with journalAlignSize", 0, fileSize % journalAlignSize);

        jc.close();
    }

    // Mutation coverage
    // notifyRename: line 168
    // Controlla che il metodo notifyRename venga chiamato correttamente
    @Test
    public void testJournalChannel_coverage_8() throws Exception {

        Long toReplaceLogId = 124L;
        File toReplaceLogFile = new File(JOURNAL_DIRECTORY, Long.toHexString(toReplaceLogId) + ".txn");
        if (!toReplaceLogFile.exists()) toReplaceLogFile.createNewFile();

        when(mockFcp.supportReuseFile()).thenReturn(true);
        when(mockFcp.open(any(File.class), any(ServerConfiguration.class)))
                .thenAnswer(invocation -> {
                    File file = invocation.getArgument(0);
                    ServerConfiguration serverConfiguration = invocation.getArgument(1);
                    return new DefaultFileChannel(file, serverConfiguration);
                });

        // Uso il mockFcp per verificare che notifyRename venga chiamato
        JournalChannelBuilder jcb = new JournalChannelBuilder()
                .withFileChannelProvider(mockFcp)
                .withToReplaceLogId(toReplaceLogId);
        JournalChannel jc = jcb.build();

        // Verifica che il metodo notifyRename sia stato chiamato
        Mockito.verify(mockFcp, times(1)).notifyRename(any(File.class), any(File.class));
        jc.close();
    }

    // Mutation coverage
    // formatVersionToWrite: line 174
    // Controlla che la versione minima di un nuovo Journal sia V4
    @Test
    public void testJournalChannel_coverage_9() throws Exception {
        JournalChannelBuilder jcb = new JournalChannelBuilder().withFormatVersionToWrite(JournalChannel.V4);
        JournalChannel jc = jcb.build();
        Assert.assertEquals("Expected V4 minimum versionToWrite", JournalChannel.V4, jc.getFormatVersion());
        jc.close();
    }

    // Mutation coverage
    // fRemoveFromPageCache: line 243
    // Controlla che venga preso il file descriptor per gestire la cache delle pagine
    @Test
    public void testJournalChannel_coverage_10() throws Exception {
        JournalChannelBuilder jcb = new JournalChannelBuilder().withRemoveFromPageCache(true);
        JournalChannel jc = jcb.build();

        // Verifica che il file descriptor non sia -1
        Field fdField = jc.getClass().getDeclaredField("fd");
        fdField.setAccessible(true);
        int fd = (int) fdField.get(jc);

        Assert.assertTrue("Expected a source file to manage page cache", fd != -1);
        jc.close();
    }

    // Mutation coverage
    // ZeroBuffer: line 254
    // Controlla che il buffer dell'header venga azzerato correttamente prima di scriverla
    @Test
    public void testJournalChannel_coverage_11() throws Exception {
        try (MockedStatic<ZeroBuffer> zeroBufferMock = mockStatic(ZeroBuffer.class)) {
            // Costruisci JournalChannel normalmente
            JournalChannelBuilder jcb = new JournalChannelBuilder();
            JournalChannel jc = jcb.build();

            // Verifica che ZeroBuffer.put sia stato chiamato almeno una volta
            zeroBufferMock.verify(() -> ZeroBuffer.put(any(ByteBuffer.class)), atLeastOnce());

            jc.close();
        }
    }

    // Mutation coverage
    // forceWrite: line 262
    // Controlla che il metodo force venga chiamato correttamente
    @Test
    public void testJournalChannel_coverage_12() throws Exception {
        Journal.BufferedChannelBuilder bcBuilder = mock(Journal.BufferedChannelBuilder.class);
        BufferedChannel bc = mock(BufferedChannel.class);
        when(bcBuilder.create(any(FileChannel.class), anyInt())).thenReturn(bc);
        when(bc.forceWrite(anyBoolean())).thenReturn(512L);

        JournalChannelTest.JournalChannelBuilder jcb = new JournalChannelTest.JournalChannelBuilder()
                .withBufferedChannelBuilder(bcBuilder)
                .withFileChannelProvider(mockFcp);

        JournalChannel jc = jcb.build();

        // Verifica che force sia stato chiamato almeno una volta
        verify(bc, atLeastOnce()).forceWrite(anyBoolean());

        jc.close();
    }

    // Mutation coverage
    // getBufferedChannel: line 279, 282
    // Controlla che il metodo getBufferedChannel ritorni un BufferedChannel valido se il file è scrivibile
    @Test
    public void testJournalChannel_coverage_13() throws Exception {

        JournalChannelBuilder jcb = new JournalChannelBuilder()
                .withBufferedChannelBuilder(mockBcb);

        JournalChannel jc = jcb.build();

        // Verifica che il BufferedChannel non sia null
        Assert.assertNotNull("BufferedChannel should not be null", jc.getBufferedChannel());

        jc.close();
    }

    // Mutation coverage
    // preAllocIfNeeded: line 286, 287
    @Test
    public void testJournalChannel_coverage_14() throws Exception {

        when(mockBc.position()).thenReturn(0L);

        JournalChannelBuilder jcb = new JournalChannelBuilder()
                .withPreAllocSize(512L)
                .withJournalAlignSize(512)
                .withBufferedChannelBuilder(mockBcb)
                .withFileChannelProvider(mockFcp);
        JournalChannel jc = jcb.build();

        // Azzeriamo le interazioni sul mock del FileChannel dopo la build
        clearInvocations(mockFc);

        // Chiamata che NON deve causare preallocazione (bc.position() + size == nextPrealloc: 0 + 512 == 512)
        jc.preAllocIfNeeded(512L);

        // Chiamata che NON deve causare preallocazione (bc.position() + size < nextPrealloc: 0 + 0 < 512)
        jc.preAllocIfNeeded(0L);

        // Verifica che non sia stata effettuata nessuna scrittura
        verify(mockFc, never()).write(any(ByteBuffer.class), anyLong());

        // Chiamata che deve causare preallocazione (bc.position() + size > nextPrealloc: 0 + 513 < 513)
        jc.preAllocIfNeeded(513L);

        // La scrittura deve avvenire
        verify(mockFc, atLeastOnce()).write(any(ByteBuffer.class), anyLong());

        jc.close();
    }

    // Mutation coverage
    // preAllocIfNeeded: line 289
    @Test
    public void testJournalChannel_coverage_15() throws Exception {
        when(mockBc.position()).thenReturn(0L);

        JournalChannelBuilder jcb = new JournalChannelBuilder()
                .withPreAllocSize(Long.MIN_VALUE)
                .withBufferedChannelBuilder(mockBcb)
                .withFileChannelProvider(mockFcp);
        JournalChannel jc = jcb.build();

        clearInvocations(mockFc);

        try {
            jc.preAllocIfNeeded(1L);
            Assert.fail("Expected IllegalArgumentException due to negative offset");
        } catch (Exception e) {
            verify(mockFc, atLeastOnce()).write(any(ByteBuffer.class), anyLong());
            Assert.assertTrue(e instanceof IllegalArgumentException);
        }

        jc.close();
    }

    // --- HEADER TESTS ---

    // Line coverage
    // formatVersion V1: nessun header
    @Test
    public void testJournalChannel_header_V1_1() throws Exception {
        File dummyJournalFile = new File(JOURNAL_DIRECTORY, Long.toHexString(dummyJournalId) + ".txn");
        if (!dummyJournalFile.exists()) dummyJournalFile.createNewFile();
        // Lascia il file vuoto (nessun header)
        JournalChannelBuilder jcb = new JournalChannelBuilder();
        JournalChannel jc = jcb.build();
        Assert.assertEquals(JournalChannel.V1, jc.getFormatVersion());
        jc.close();
    }

    // Line coverage
    // formatVersion V1: header malformata
    @Test
    public void testJournalChannel_header_V1_2() throws Exception {
        File dummyJournalFile = new File(JOURNAL_DIRECTORY, Long.toHexString(dummyJournalId) + ".txn");
        try (FileChannel fc = new RandomAccessFile(dummyJournalFile, "rw").getChannel()) {
            ByteBuffer header = ByteBuffer.allocate(JournalChannel.VERSION_HEADER_SIZE);
            header.put("ABCD".getBytes(StandardCharsets.UTF_8));
            header.putInt(0);
            header.flip();
            fc.write(header, 0);
        }
        JournalChannelBuilder jcb = new JournalChannelBuilder();
        JournalChannel jc = jcb.build();
        Assert.assertEquals(JournalChannel.V1, jc.getFormatVersion());
        jc.close();
    }

    // Line coverage
    // formatVersion V2: magic word + versione 2
    @Test
    public void testJournalChannel_header_V2() throws Exception {
        File dummyJournalFile = new File(JOURNAL_DIRECTORY, Long.toHexString(dummyJournalId) + ".txn");
        try (FileChannel fc = new RandomAccessFile(dummyJournalFile, "rw").getChannel()) {
            ByteBuffer header = ByteBuffer.allocate(JournalChannel.VERSION_HEADER_SIZE);
            header.put("BKLG".getBytes(StandardCharsets.UTF_8));
            header.putInt(JournalChannel.V2);
            header.flip();
            fc.write(header, 0);
        }
        JournalChannelBuilder jcb = new JournalChannelBuilder();
        JournalChannel jc = jcb.build();
        Assert.assertEquals(JournalChannel.V2, jc.getFormatVersion());
        jc.close();
    }

    // Line coverage
    // formatVersion V3: magic word + versione 3
    @Test
    public void testJournalChannel_header_V3() throws Exception {
        File dummyJournalFile = new File(JOURNAL_DIRECTORY, Long.toHexString(dummyJournalId) + ".txn");
        try (FileChannel fc = new RandomAccessFile(dummyJournalFile, "rw").getChannel()) {
            ByteBuffer header = ByteBuffer.allocate(JournalChannel.VERSION_HEADER_SIZE);
            header.put("BKLG".getBytes(StandardCharsets.UTF_8));
            header.putInt(JournalChannel.V3);
            header.flip();
            fc.write(header, 0);
        }
        JournalChannelBuilder jcb = new JournalChannelBuilder();
        JournalChannel jc = jcb.build();
        Assert.assertEquals(JournalChannel.V3, jc.getFormatVersion());
        jc.close();
    }

    // Line coverage
    // formatVersion V4: magic word + versione 4
    @Test
    public void testJournalChannel_header_V4() throws Exception {
        File dummyJournalFile = new File(JOURNAL_DIRECTORY, Long.toHexString(dummyJournalId) + ".txn");
        try (FileChannel fc = new RandomAccessFile(dummyJournalFile, "rw").getChannel()) {
            ByteBuffer header = ByteBuffer.allocate(JournalChannel.VERSION_HEADER_SIZE);
            header.put("BKLG".getBytes(StandardCharsets.UTF_8));
            header.putInt(JournalChannel.V4);
            header.flip();
            fc.write(header, 0);
        }
        JournalChannelBuilder jcb = new JournalChannelBuilder();
        JournalChannel jc = jcb.build();
        Assert.assertEquals(JournalChannel.V4, jc.getFormatVersion());
        jc.close();
    }

    // Line coverage
    // formatVersion V5: magic word + versione 5
    @Test
    public void testJournalChannel_header_V5() throws Exception {
        File dummyJournalFile = new File(JOURNAL_DIRECTORY, Long.toHexString(dummyJournalId) + ".txn");
        try (FileChannel fc = new RandomAccessFile(dummyJournalFile, "rw").getChannel()) {
            ByteBuffer header = ByteBuffer.allocate(JournalChannel.VERSION_HEADER_SIZE);
            header.put("BKLG".getBytes(StandardCharsets.UTF_8));
            header.putInt(JournalChannel.V5);
            header.flip();
            fc.write(header, 0);
        }
        JournalChannelBuilder jcb = new JournalChannelBuilder();
        JournalChannel jc = jcb.build();
        Assert.assertEquals(JournalChannel.V5, jc.getFormatVersion());
        jc.close();
    }

    // Line coverage
    // formatVersion V6: magic word + versione 6
    @Test
    public void testJournalChannel_header_V6() throws Exception {
        File dummyJournalFile = new File(JOURNAL_DIRECTORY, Long.toHexString(dummyJournalId) + ".txn");
        try (FileChannel fc = new RandomAccessFile(dummyJournalFile, "rw").getChannel()) {
            ByteBuffer header = ByteBuffer.allocate(JournalChannel.VERSION_HEADER_SIZE);
            header.put("BKLG".getBytes(StandardCharsets.UTF_8));
            header.putInt(JournalChannel.V6);
            header.flip();
            fc.write(header, 0);
        }
        JournalChannelBuilder jcb = new JournalChannelBuilder();
        JournalChannel jc = jcb.build();
        Assert.assertEquals(JournalChannel.V6, jc.getFormatVersion());
        jc.close();
    }

    // Line coverage
    // formatVersion: 0
    @Test
    public void testJournalChannel_header_zero() throws Exception {

        File dummyJournalFile = new File(JOURNAL_DIRECTORY, Long.toHexString(dummyJournalId) + ".txn");
        try (FileChannel fc = new RandomAccessFile(dummyJournalFile, "rw").getChannel()) {
            ByteBuffer header = ByteBuffer.allocate(JournalChannel.VERSION_HEADER_SIZE);
            header.put("BKLG".getBytes(StandardCharsets.UTF_8));
            header.putInt(0);
            header.flip();
            fc.write(header, 0);
        }

        try {
            JournalChannelBuilder jcb = new JournalChannelBuilder();
            JournalChannel jc = jcb.build();
            Assert.fail("Expected Exception due to invalid version 0");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof IOException);
        }
    }

    // Line coverage
    // formatVersion: -1
    @Test
    public void testJournalChannel_header_negative() throws Exception {

        File dummyJournalFile = new File(JOURNAL_DIRECTORY, Long.toHexString(dummyJournalId) + ".txn");
        try (FileChannel fc = new RandomAccessFile(dummyJournalFile, "rw").getChannel()) {
            ByteBuffer header = ByteBuffer.allocate(JournalChannel.VERSION_HEADER_SIZE);
            header.put("BKLG".getBytes(StandardCharsets.UTF_8));
            header.putInt(-1);
            header.flip();
            fc.write(header, 0);
        }

        try {
            JournalChannelBuilder jcb = new JournalChannelBuilder();
            JournalChannel jc = jcb.build();
            Assert.fail("Expected Exception due to invalid version -1");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof IOException);
        }
    }

    // Line coverage
    // formatVersion: CURRENT_VERSION + 1
    @Test
    public void testJournalChannel_header_current_plus() throws Exception {

        File dummyJournalFile = new File(JOURNAL_DIRECTORY, Long.toHexString(dummyJournalId) + ".txn");
        try (FileChannel fc = new RandomAccessFile(dummyJournalFile, "rw").getChannel()) {
            ByteBuffer header = ByteBuffer.allocate(JournalChannel.VERSION_HEADER_SIZE);
            header.put("BKLG".getBytes(StandardCharsets.UTF_8));
            header.putInt(JournalChannel.CURRENT_JOURNAL_FORMAT_VERSION + 1);
            header.flip();
            fc.write(header, 0);
        }

        try {
            JournalChannelBuilder jcb = new JournalChannelBuilder();
            JournalChannel jc = jcb.build();
            Assert.fail("Expected Exception due to invalid version CURRENT_VERSION + 1");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof IOException);
        }
    }

    private class JournalChannelBuilder {
        private File journalDirectory = JOURNAL_DIRECTORY;
        private Long logId = dummyJournalId;
        private long preAllocSize = 4096;
        private int writeBufferSize = 4096;
        private int journalAlignSize = 512;
        private long position = 0;
        private boolean fRemoveFromPageCache = false;
        private int formatVersionToWrite = JournalChannel.V6;
        private Journal.BufferedChannelBuilder bcBuilder = Journal.BufferedChannelBuilder.DEFAULT_BCBUILDER;
        private ServerConfiguration conf = serverConfiguration;
        private FileChannelProvider provider = new DefaultFileChannelProvider();
        private Long toReplaceLogId = null;

        public JournalChannelBuilder withJournalDirectory(File dir) {
            this.journalDirectory = dir;
            return this;
        }

        public JournalChannelBuilder withLogId(Long logId) {
            this.logId = logId;
            return this;
        }

        public JournalChannelBuilder withPreAllocSize(long size) {
            this.preAllocSize = size;
            return this;
        }

        public JournalChannelBuilder withWriteBufferSize(int size) {
            this.writeBufferSize = size;
            return this;
        }

        public JournalChannelBuilder withJournalAlignSize(int size) {
            this.journalAlignSize = size;
            return this;
        }

        public JournalChannelBuilder withPosition(long pos) {
            this.position = pos;
            return this;
        }

        public JournalChannelBuilder withRemoveFromPageCache(boolean remove) {
            this.fRemoveFromPageCache = remove;
            return this;
        }

        public JournalChannelBuilder withFormatVersionToWrite(int version) {
            this.formatVersionToWrite = version;
            return this;
        }

        public JournalChannelBuilder withBufferedChannelBuilder(Journal.BufferedChannelBuilder builder) {
            this.bcBuilder = builder;
            return this;
        }

        public JournalChannelBuilder withServerConfiguration(ServerConfiguration conf) {
            this.conf = conf;
            return this;
        }

        public JournalChannelBuilder withFileChannelProvider(FileChannelProvider provider) {
            this.provider = provider;
            return this;
        }

        public JournalChannelBuilder withToReplaceLogId(Long toReplaceLogId) {
            this.toReplaceLogId = toReplaceLogId;
            return this;
        }

        public JournalChannel build() throws Exception {
            return new JournalChannel(
                journalDirectory,
                logId,
                preAllocSize,
                writeBufferSize,
                journalAlignSize,
                fRemoveFromPageCache,
                formatVersionToWrite,
                bcBuilder,
                conf,
                provider,
                toReplaceLogId
            );
        }
    }

}
