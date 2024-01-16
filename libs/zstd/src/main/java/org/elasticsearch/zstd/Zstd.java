/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.zstd;

public final class Zstd {

    private enum OS {
        // Even on Windows, the default compiler from cpptasks (gcc) uses .so as a shared lib extension
        WINDOWS("win32", "so"),
        LINUX("linux", "so"),
        MAC("darwin", "dylib"),
        SOLARIS("solaris", "so");

        public final String name, libExtension;

        private OS(String name, String libExtension) {
            this.name = name;
            this.libExtension = libExtension;
        }
    }

    private static String arch() {
        return System.getProperty("os.arch");
    }

    private static OS os() {
        String osName = System.getProperty("os.name");
        if (osName.contains("Linux")) {
            return OS.LINUX;
        } else if (osName.contains("Mac")) {
            return OS.MAC;
        } else if (osName.contains("Windows")) {
            return OS.WINDOWS;
        } else if (osName.contains("Solaris") || osName.contains("SunOS")) {
            return OS.SOLARIS;
        } else {
            throw new UnsupportedOperationException("Unsupported operating system: " + osName);
        }
    }

    private static String libPath() {
        OS os = os();
        return System.getProperty("user.dir")
            + "/build/jni/org/elasticsearch/zstd/"
            + os.name
            + "/"
            + arch()
            + "/libzstd-jni."
            + os.libExtension;
    }

    static {
        System.load(libPath());
    }

    /**
     * Compress {@code src[srcOff:srcOff+srcLen]} into {@code dst[dstOff:]} using the given compression level. It is recommended that
     * {@code dst} has at least {@link Zstd#maxCompressedLength} bytes available for the compressed content.
     */
    public static native int compress(byte[] dst, int dstOff, byte[] src, int srcOff, int srcLen, int level);

    /**
     * Decompress {@code src[srcOff:srcOff+srcLen]} into {@code dst[dstOff:]}.
     */
    public static native int decompress(byte[] dst, int dstOff, byte[] src, int srcOff, int srcLen);

    /**
     * Return the maximum compressed length for the given input length.
     */
    public static native int maxCompressedLength(int srcLen);

    private Zstd() {}

}
