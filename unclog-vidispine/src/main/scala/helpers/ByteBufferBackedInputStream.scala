package helpers

import java.io.{IOException, InputStream}

//adapted from https://stackoverflow.com/questions/4332264/wrapping-a-bytebuffer-with-an-inputstream
class ByteBufferBackedInputStream(val buf: java.nio.ByteBuffer) extends InputStream {
  @throws[IOException]
  def read: Int = {
    if (!buf.hasRemaining) return -1
    buf.get & 0xFF
  }

  @throws[IOException]
  override def read(bytes: Array[Byte], off: Int, requestedLength: Int): Int = {
    if (!buf.hasRemaining) return -1
    val actualLength = Math.min(requestedLength, buf.remaining)
    buf.get(bytes, off, actualLength)
    actualLength
  }
}
