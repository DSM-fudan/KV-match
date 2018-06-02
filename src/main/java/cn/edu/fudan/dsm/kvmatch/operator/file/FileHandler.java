/*
 * Copyright 2017 Jiaye Wu
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cn.edu.fudan.dsm.kvmatch.operator.file;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

/**
 * A class to manage the file I/O operations.
 * <p>
 * Created by Jiaye Wu on 17-8-24.
 */
public class FileHandler implements Closeable {

    private static final Logger logger = LoggerFactory.getLogger(FileHandler.class.getName());

    // common
    private String mode;
    private File file;

    // for read-only mode ("r")
    private RandomAccessFile reader;

    // for write-only mode ("w")
    private BufferedOutputStream writer;

    /**
     * Create the file i/o instance. Note that the instance can be either used to write or read.
     *
     * @param filePath the target file.
     * @param mode     i/o mode ("w": write, "r": read).
     */
    public FileHandler(String filePath, String mode) throws IOException {
        this.mode = mode;
        this.file = new File(filePath);
        switch (mode) {
            case "r":
                this.reader = new RandomAccessFile(file, mode);
                break;
            case "w":
                FileUtils.forceMkdirParent(file);
                this.writer = new BufferedOutputStream(new FileOutputStream(file));
                break;
            default:
                throw new IllegalArgumentException("Only support read_only(r) and write_only(w) modes.");
        }
    }

    /**
     * Return the file instance to support advanced operations, e.g. get file size.
     *
     * @return the file instance associate with this handler
     */
    public File getFile() {
        return file;
    }

    /**
     * Seek to specific position of the file, and read the following number of bytes.
     *
     * @param pos           the start position
     * @param lengthOfBytes the number of bytes should be read (can not larger than INT_MAX = 2GB)
     * @return a byte array containing the bytes read
     * @throws IOException if any error occurred during the read process
     */
    public byte[] read(long pos, int lengthOfBytes) throws IOException {
        if (!mode.equals("r")) {
            throw new IllegalStateException("The file handler is not instanced for read.");
        }
        byte[] bytes = new byte[lengthOfBytes];
        reader.seek(pos);
        reader.read(bytes, 0, lengthOfBytes);
        return bytes;
    }

    /**
     * Append the bytes to the tail of the file.
     *
     * @param bytes the bytes need to be written
     * @throws IOException if any error occurred during the write process
     */
    public void write(byte[] bytes) throws IOException {
        if (!mode.equals("w")) {
            throw new IllegalStateException("The file handler is not instanced for write.");
        }
        writer.write(bytes, 0, bytes.length);
        writer.flush();
    }

    @Override
    public void close() throws IOException {
        switch (mode) {
            case "r":
                reader.close();
                break;
            case "w":
                writer.close();
                break;
            default:
                break;
        }
    }
}
