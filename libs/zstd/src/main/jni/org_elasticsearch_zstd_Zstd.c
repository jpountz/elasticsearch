#include <jni.h>
#include <zstd.h>

static size_t MAX_INT = (((size_t) 1) << 31) - 1;

static jint throwIllegalArgumentException(JNIEnv *env, const char *msg) {
  jclass clazz = (*env)->FindClass(env, "java/lang/IllegalArgumentException");
  return (*env)->ThrowNew(env, clazz, msg);
}

static jint throwArithmeticException(JNIEnv *env, const char *msg) {
  jclass clazz = (*env)->FindClass(env, "java/lang/ArithmeticException");
  return (*env)->ThrowNew(env, clazz, msg);
}

/*
 * Class:     org_elasticsearch_zstd_Zstd
 * Method:    compress
 * Signature: ([BI[BIII)I
 */
JNIEXPORT jint JNICALL Java_org_elasticsearch_zstd_Zstd_compress
  (JNIEnv* env, jclass cls, jbyteArray dst, jint dstOff, jbyteArray src, jint srcOff, jint srcLen, jint level) {

  if (dst == NULL) {
    return throwIllegalArgumentException(env, "Null dst");
  }
  if (src == NULL) {
    return throwIllegalArgumentException(env, "Null src");
  }
  if (srcLen < 0) {
    return throwIllegalArgumentException(env, "Negative srcLen");
  }
  if (srcOff < 0) {
    return throwIllegalArgumentException(env, "Negative srcOff");
  }
  if (dstOff < 0) {
    return throwIllegalArgumentException(env, "Negative dstOff");
  }

  jsize dstCapacity = (*env)->GetArrayLength(env, dst);
  jsize srcCapacity = (*env)->GetArrayLength(env, src);

  if (dstCapacity < dstOff) {
    return throwIllegalArgumentException(env, "Out of bounds dstOff");
  }

  if (srcCapacity < (size_t) srcOff + srcLen) {
    return throwIllegalArgumentException(env, "Out of bounds srcOff + srcLen");
  }

  // Entering critical section, don't do I/O or wait on other threads
  jbyte* in = (*env)->GetPrimitiveArrayCritical(env, src, JNI_FALSE);
  jbyte* out = (*env)->GetPrimitiveArrayCritical(env, dst, JNI_FALSE);

  size_t ret = ZSTD_compress(out + dstOff, dstCapacity - dstOff, in + srcOff, srcLen, level);

  (*env)->ReleasePrimitiveArrayCritical(env, dst, out, JNI_FALSE);
  (*env)->ReleasePrimitiveArrayCritical(env, src, in, JNI_FALSE);
  // Exiting critical section

  if (ZSTD_isError(ret)) {
    return throwIllegalArgumentException(env, ZSTD_getErrorName(ret));
  }

  return (jint) ret;
}

/*
 * Class:     org_elasticsearch_zstd_Zstd
 * Method:    decompress
 * Signature: ([BI[BII)I
 */
JNIEXPORT jint JNICALL Java_org_elasticsearch_zstd_Zstd_decompress
  (JNIEnv* env, jclass cls, jbyteArray dst, jint dstOff, jbyteArray src, jint srcOff, jint srcLen) {

  if (dst == NULL) {
    return throwIllegalArgumentException(env, "Null dst");
  }
  if (src == NULL) {
    return throwIllegalArgumentException(env, "Null src");
  }
  if (srcLen < 0) {
    return throwIllegalArgumentException(env, "Negative srcLen");
  }
  if (srcOff < 0) {
    return throwIllegalArgumentException(env, "Negative srcOff");
  }
  if (dstOff < 0) {
    return throwIllegalArgumentException(env, "Negative dstOff");
  }

  jsize dstCapacity = (*env)->GetArrayLength(env, dst);
  jsize srcCapacity = (*env)->GetArrayLength(env, src);

  if (dstCapacity < dstOff) {
    return throwIllegalArgumentException(env, "Out of bounds dstOff");
  }

  if (srcCapacity < (size_t) srcOff + srcLen) {
    return throwIllegalArgumentException(env, "Out of bounds srcOff + srcLen");
  }

  // Entering critical section, don't do I/O or wait on other threads
  jbyte* in = (*env)->GetPrimitiveArrayCritical(env, src, JNI_FALSE);
  jbyte* out = (*env)->GetPrimitiveArrayCritical(env, dst, JNI_FALSE);

  size_t ret = ZSTD_decompress(out + dstOff, dstCapacity - dstOff, in + srcOff, srcLen);

  (*env)->ReleasePrimitiveArrayCritical(env, dst, out, JNI_FALSE);
  (*env)->ReleasePrimitiveArrayCritical(env, src, in, JNI_FALSE);
  // Exiting critical section

  if (ZSTD_isError(ret)) {
    return throwIllegalArgumentException(env, ZSTD_getErrorName(ret));
  }

  return ret;
}

/*
 * Class:     org_elasticsearch_zstd_Zstd
 * Method:    maxCompressedLength
 * Signature: (I)I
 */
JNIEXPORT jint JNICALL Java_org_elasticsearch_zstd_Zstd_maxCompressedLength
  (JNIEnv *env, jclass, jint srcLen) {

  if (srcLen < 0) {
    return throwIllegalArgumentException(env, "Negative srcLen");
  }

  size_t bound = ZSTD_compressBound((size_t) srcLen);

  if (bound < 0 || bound > MAX_INT) {
    return throwArithmeticException(env, "srcLen may require more than Integer.MAX_VALUE bytes once compressed");
  }

  return (jint) bound;
}
