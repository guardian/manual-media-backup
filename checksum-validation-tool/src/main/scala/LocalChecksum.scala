import java.io.{File, FileInputStream}
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.security.MessageDigest

import models.ObjectMatrixEntry
import org.apache.commons.codec.binary.Hex
import org.slf4j.LoggerFactory

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Success, Try}

object LocalChecksum {
  private val logger = LoggerFactory.getLogger(getClass)

  def findFile(forEntry:ObjectMatrixEntry) = forEntry.pathOrFilename.flatMap(filePath=>{
    val f = new File(filePath)
    if(f.exists()){
      Some(f)
    } else {
      None
    }
  })

  private def md5NextChunk(stream:FileInputStream, chunkNum:Int, chunkSize:Long, lastChunkSize:Long, totalChunks:Int, digester:MessageDigest):MessageDigest = {
    if(chunkNum>totalChunks) return digester

      logger.debug(s"reading chunk $chunkNum of $totalChunks")
      val buffer = if (chunkNum < totalChunks) {
        stream.getChannel.map(FileChannel.MapMode.READ_ONLY, chunkNum.toLong * chunkSize.toLong, chunkSize)
      } else {
        stream.getChannel.map(FileChannel.MapMode.READ_ONLY, chunkNum.toLong * chunkSize.toLong, lastChunkSize)
      }
      digester.update(buffer)
      md5NextChunk(stream, chunkNum+1, chunkSize,lastChunkSize, totalChunks, digester)
  }

  /**
    * determines chunk sizes for the file. If less than 5M one single chunk is returned for the whole file
    * If greater, then chunks of 5M are returned
    * @param forFile
    * @return a Tuple of Longs, consisiting of :- (chunkSize, chunkCount, lastChunkSize)
    */
  private def findChunkSize(forFile:File): (Long,Long,Long) = {
    if(forFile.length()<5242880) { //1Mb
      (forFile.length(), 1, forFile.length())
    } else {
      val chunkCount = forFile.length() / 5242880
      val lastChunkSize = forFile.length() % 5242880
      (5242880L, chunkCount, lastChunkSize)
    }
  }

  def calculateChecksum(forFile:File)(implicit exec:ExecutionContext) = {
    val (chunkSize, chunkCount, lastChunkSize) = findChunkSize(forFile)

    Try { new FileInputStream(forFile) } flatMap { stream=> Try {
      val digester = MessageDigest.getInstance("md5")
      val finalDigester = md5NextChunk(stream, 0, chunkSize, lastChunkSize, chunkCount.toInt, digester)

      Hex.encodeHexString(finalDigester.digest())
    }}
  }
}
