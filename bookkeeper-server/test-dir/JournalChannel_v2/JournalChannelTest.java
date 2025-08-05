package org.apache.bookkeeper.bookie;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
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

    @Before
    public void setUp() throws Exception {

        JOURNAL_DIRECTORY.mkdirs();
        dummyJournalId = UUID.randomUUID().getLeastSignificantBits();
        serverConfiguration.setJournalDirName(JOURNAL_DIRECTORY.getAbsolutePath());

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

        FileChannelProvider mockedFcp = mock(DefaultFileChannelProvider.class);
        when(mockedFcp.supportReuseFile()).thenReturn(true);
        when(mockedFcp.open(any(File.class), any(ServerConfiguration.class)))
            .thenAnswer(invocation -> {
                File file = invocation.getArgument(0);
                ServerConfiguration serverConfiguration = invocation.getArgument(1);
                return new DefaultFileChannel(file, serverConfiguration);
            });

        JournalChannelBuilder jcb = new JournalChannelBuilder()
                .withToReplaceLogId(toReplaceLogId)
                .withFileChannelProvider(mockedFcp);
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

        FileChannelProvider mockedFcp = mock(DefaultFileChannelProvider.class);
        when(mockedFcp.supportReuseFile()).thenReturn(true);
        when(mockedFcp.open(any(File.class), any(ServerConfiguration.class)))
            .thenAnswer(invocation -> {
                File file = invocation.getArgument(0);
                ServerConfiguration serverConfiguration = invocation.getArgument(1);
                return new DefaultFileChannel(file, serverConfiguration);
            });

        JournalChannelBuilder jcb = new JournalChannelBuilder()
                .withToReplaceLogId(toReplaceLogId)
                .withFileChannelProvider(mockedFcp);
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

        // Mock PageCacheUtil
        // Usa PowerMockito o altro framework se serve mockare statici, oppure verifica con side effect

        JournalChannelBuilder jcb = new JournalChannelBuilder().withBufferedChannelBuilder(bcBuilder).withRemoveFromPageCache(true);
        JournalChannel jc = jcb.build();

        // Forza la scrittura
        jc.forceWrite(false);

        // Verifica che lastDropPosition sia stato aggiornato (>0)
        Field lastDropPosition = jc.getClass().getDeclaredField("lastDropPosition");
        lastDropPosition.setAccessible(true);
        Assert.assertTrue(lastDropPosition.getLong(jc) > 0);
    }

    // Line coverage
    @Test
    public void testJournalChannel_coverage_4() throws Exception {
        FileChannel mockFc = mock(FileChannel.class);
        lenient().when(mockFc.position(anyLong())).thenThrow(new IOException());

        BookieFileChannel mockBfc = mock(BookieFileChannel.class);
        lenient().when(mockBfc.getFileChannel()).thenReturn(mockFc);

        FileChannelProvider provider = mock(FileChannelProvider.class);
        when(provider.open(any(File.class), any(ServerConfiguration.class))).thenReturn(mockBfc);

        File dummyJournalFile = new File(JOURNAL_DIRECTORY, Long.toHexString(dummyJournalId) + ".txn");
        if (!dummyJournalFile.exists()) dummyJournalFile.createNewFile();

        try {
            JournalChannelBuilder jcb = new JournalChannelBuilder()
                    .withFileChannelProvider(provider);
            jcb.build();
            Assert.fail("Expected IOException");
        } catch (IOException e) {
            Assert.assertTrue(e instanceof IOException);
        }
    }

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
        private int preAllocSize = 4096;
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

        public JournalChannelBuilder withPreAllocSize(int size) {
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
