package helpers

import java.io.File
import java.nio.file.{Files, LinkOption}
import java.nio.file.attribute.{BasicFileAttributes, FileTime}
import java.time.temporal.TemporalField
import java.time.{Instant, ZoneId, ZonedDateTime}

import akka.stream.{ClosedShape, Materializer, SourceShape}
import akka.stream.scaladsl.{GraphDSL, RunnableGraph, Sink}
import com.om.mxs.client.japi.{MatrixStore, MxsObject, SearchTerm, UserInfo, Vault}
import models.{MxsMetadata, ObjectMatrixEntry}
import org.slf4j.LoggerFactory
import streamcomponents.{OMLookupMetadata, OMSearchSource}

import scala.util.{Failure, Success, Try}
import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.matching.Regex

object MatrixStoreHelper {
  private val logger = LoggerFactory.getLogger(getClass)

  def openVault(userInfo:UserInfo):Try[Vault] = Try {
    MatrixStore.openVault(userInfo)
  }

  /**
    * locate files for the given filename, as stored in the metadata. This assumes that one or at most two records will
    * be returned and should therefore be more efficient than using the streaming interface. If many records are expected,
    * this will be inefficient and you should use the streaming interface
    * @param vault MXS `vault` object
    * @param fileName file name to search for
    * @return a Try, containing either a sequence of zero or more results as [[ObjectMatrixEntry]] records or an error
    */
  def findByFilename(vault:Vault, fileName:String):Try[Seq[ObjectMatrixEntry]] = Try {
    val searchTerm = SearchTerm.createSimpleTerm("MXFS_PATH",fileName) //FIXME: check the metadata field namee
    val iterator = vault.searchObjectsIterator(searchTerm, 1).asScala

    var finalSeq:Seq[ObjectMatrixEntry] = Seq()
    while(iterator.hasNext){
      finalSeq ++= Seq(ObjectMatrixEntry(iterator.next(), vault, None, None))
    }
    finalSeq
  }

  /**
    * helper function to initialise a Source that finds elements matching the given name and looks up their metadata.
    * both of these operations are performed with async barriers
    * @param userInfo MXS UserInfo object that provides cluster, login and vault details
    * @param searchTerms search terms to search for, as MXS SearchTerms object
    * @param mat implicitly provided actor materializer
    * @param ec implicitly provided execution context
    * @return a partial graph that provides a Source to be mixed into another stream
    */
  def findBulkSource(userInfo:UserInfo, searchTerms:SearchTerm)(implicit mat:Materializer, ec:ExecutionContext) = {
    GraphDSL.create() { implicit builder=>
      import akka.stream.scaladsl.GraphDSL.Implicits._

      val src = builder.add(new OMSearchSource(userInfo,searchTerms).async)
      val mdLookup = builder.add(new OMLookupMetadata().async)

      src ~> mdLookup

      SourceShape(mdLookup.out)
    }
  }

  /**
    * helper function to perform a filename search using the streaming interface
    * @param userInfo  MXS UserInfo object that provides cluster, login and vault details
    * @param fileName file name to search for
    * @param mat implicitly provided actor materializer
    * @param ec implicitly provided execution context
    * @return a Future, containing a Sequence of matching [[ObjectMatrixEntry]] records. If the stream fails then
    *         the future is cancelled; use either .onComplete or .recover/.recoverWith to handle this.
    */
  def findByFilenameBulk(userInfo:UserInfo, fileName:String)(implicit mat:Materializer, ec:ExecutionContext) = {
    val sinkFactory = Sink.fold[Seq[ObjectMatrixEntry],ObjectMatrixEntry](Seq())((acc,entry)=>acc ++ Seq(entry))
    val searchTerm = SearchTerm.createSimpleTerm("filename",fileName)

    val graph = GraphDSL.create(sinkFactory) { implicit builder=> sink=>
      import akka.stream.scaladsl.GraphDSL.Implicits._
      val src = findBulkSource(userInfo, searchTerm)

      src ~> sink
      ClosedShape
    }

    RunnableGraph.fromGraph(graph).run()
  }

  /**
    * returns the file extension of the provided filename, or None if there is no extension
    * @param fileNameString filename string
    * @return the content of the last extension
    */
  def getFileExt(fileNameString:String):Option[String] = {
    val re = ".*\\.([^\\.]+)$".r

    fileNameString match {
      case re(xtn) =>
        if (xtn.length < 8) {
          Some(xtn)
        } else {
          logger.warn(s"$xtn does not look like a file extension (too long), assuming no actual extension")
          None
        }
      case _ => None
    }
  }

  /**
    * converts mime type into a category integer, as per MatrixStoreAdministrationProgrammingGuidelines.pdf p.9
    * @param mt MIME type as string
    * @return an integer
    */
  val mimeTypeRegex = "^([^\\/]+)/(.*)$".r

  def categoryForMimetype(mt: Option[String]):Int = mt match {
    case None=>
      logger.warn(s"No MIME type provided!")
      0
    case Some(mimeTypeRegex("video",minor)) =>2
    case Some(mimeTypeRegex("audio",minor)) =>3
    case Some(mimeTypeRegex("document",minor)) =>4
    case Some(mimeTypeRegex("application",minor)) =>4
    case Some(mimeTypeRegex("image",minor))=>5
    case Some(mimeTypeRegex(major,minor))=>
      logger.info(s"Did not regognise major type $major (minor was $minor)")
      0
  }
  /*
    * Map(MXFS_FILENAME_UPPER -> GDN_ZCO_110103_VIDEO_001.XML.BZ2,
    * _fs_compressed -> ,
    * DPSP_TIMEBASEDUID -> 1fd406ca-315c-11e6-b7cd-90917ea9d7600000115,
    * MXFS_CREATIONDAY -> 10,
    * _fs_type -> ,
    * MXFS_MODIFICATION_TIME -> 1465571182000,
    * _fs_110103 -> ,
    * _fs_fs -> ,
    * server -> damserver,
    * DPSP_TIMESTAMP -> 1465818256012,
    * MXFS_CATEGORY -> 0,
    * MXFS_ARCHMONTH -> 6,
    * _fs_001 -> ,
    * MXFS_COMPATIBLE -> 1,
    * _fs_zco -> ,
    * _fs_file -> ,
    * MXFS_PARENTOID -> 0582342e-315c-11e6-a3bc-bc113b8044e7-0,
    * _fs_application -> ,
    * _fs_anonymous -> ,
    * MXFS_USERNAME -> andy_gallagher,
    * _fs_bz2 -> ,
    * _fs_gdn -> ,
    * editbox -> source_server=damserver,
    * content_type=fcp_xml_compressed,
    * _fs_editbox -> ,
    * MXFS_ARCHYEAR -> 2016,
    * __mdef__UUID -> ,
    * _fs_video -> ,
    * _fs_server -> ,
    * MXFS_PATH -> /projectxmls/gdn_zco_110103_video_001.xml.bz2,
    * _fs_bzip2 -> ,
    * MXFS_CREATION_TIME -> 1465571182000,
    * _fs_andy_gallagher -> ,
    * _fs_uuid -> ,
    * MXFS_ACCESS_TIME -> 1470740367620,
    * _fs_mdef -> ,
    * DPSP_SIZE -> 15809,
    * MXFS_GENERATOR -> DPSP,
    * MXFS_ARCHDAY -> 13,
    * MXFS_FILENAME -> gdn_zco_110103_video_001.xml.bz2,
    * _fs_x -> ,
    * _fs_source -> ,
    * MXFS_ARCHIVE_TIME -> 1465818209431,
    * _fs_xml -> ,
    * MXFS_CREATIONMONTH -> 6,
    * MXFS_MIMETYPE -> application/x-bzip2,
    * type -> fcp,
    * MXFS_CREATIONYEAR -> 2016,
    * MXFS_DESCRIPTION -> File gdn_zco_110103_video_001.xml.bz2,
    * MXFS_INTRASH -> false,
    * _fs_content -> ,
    * MXFS_FILEEXT -> bz2)
    * ),Some(FileAttributes(08271581-315c-11e6-a3bc-bc113b8044e7-5,gdn_zco_110103_video_001.xml.bz2,0582342e-315c-11e6-a3bc-bc113b8044e7-0,false,false,true,false,2016-06-13T11:43:29.431Z[UTC],2016-06-13T11:43:29.431Z[UTC],1970-01-01T00:00Z[UTC],15809))
    *
    */
  /** initialises an MxsMetadata object from filesystem metadata. Use when uploading files to matrixstore/
    * @param file java.io.File object to check
    * @return either an MxsMetadata object or an error
    */
  def metadataFromFilesystem(file:File):Try[MxsMetadata] = Try {
    val path = file.getAbsoluteFile.toPath
    val mimeType = Option(Files.probeContentType(file.toPath))

    val fsAttrs = Files.readAttributes(path,"*",LinkOption.NOFOLLOW_LINKS).asScala

    val maybeCtime = fsAttrs.get("creationTime").map(value=>ZonedDateTime.ofInstant(value.asInstanceOf[FileTime].toInstant,ZoneId.of("UTC")))
    val nowTime = ZonedDateTime.now()

    val uid = Files.getAttribute(path, "unix:uid", LinkOption.NOFOLLOW_LINKS).asInstanceOf[Int]
    MxsMetadata(
      stringValues = Map(
        "MXFS_FILENAME_UPPER" -> path.getFileName.toString.toUpperCase,
        "MXFS_FILENAME"->path.getFileName.toString,
        "MXFS_PATH"->path.toString,
        "MXFS_USERNAME"->uid.toString, //stored as a string for compatibility. There seems to be no easy way to look up the numeric UID in java/scala
        "MXFS_MIMETYPE"->mimeType.getOrElse("application/octet-stream"),
        "MXFS_DESCRIPTION"->s"File ${path.getFileName.toString}",
        "MXFS_PARENTOID"->"",
        "MXFS_FILEEXT"->getFileExt(path.getFileName.toString).getOrElse("")
      ),
      boolValues = Map(
        "MXFS_INTRASH"->false,
      ),
      longValues = Map(
        "DPSP_SIZE"->file.length(),
        "MXFS_MODIFICATION_TIME"->fsAttrs.get("lastModifiedTime").map(_.asInstanceOf[FileTime].toMillis).getOrElse(0),
        "MXFS_CREATION_TIME"->fsAttrs.get("creationTime").map(_.asInstanceOf[FileTime].toMillis).getOrElse(0),
        "MXFS_ACCESS_TIME"->fsAttrs.get("lastAccessTime").map(_.asInstanceOf[FileTime].toMillis).getOrElse(0),
      ),
      intValues = Map(
        "MXFS_CREATIONDAY"->maybeCtime.map(ctime=>ctime.getDayOfMonth).getOrElse(0),
        "MXFS_COMPATIBLE"->1,
        "MXFS_CREATIONMONTH"->maybeCtime.map(_.getMonthValue).getOrElse(0),
        "MXFS_CREATIONYEAR"->maybeCtime.map(_.getYear).getOrElse(0),
        "MXFS_CATEGORY"->categoryForMimetype(mimeType)
      )
    )
  }

  /** initialises an MxsMetadata object from filesystem metadata. Use when uploading files to matrixstore/
    * @param filepath filepath to check as a string. This is converted to a java.io.File and the other implementation is then called
    * @return either an MxsMetadata object or an error
    */
  def metadataFromFilesystem(filepath:String):Try[MxsMetadata] = metadataFromFilesystem(new File(filepath))

  protected def convertBytesToHex(bytes: Seq[Byte]): String = {
    val sb = new StringBuilder
    for (b <- bytes) {
      sb.append(String.format("%02x", Byte.box(b)))
    }
    sb.toString
  }

  /**
    * request MD5 checksum of the given object, as calculated by the appliance.
    * as per the MatrixStore documentation, a blank string implies that the digest is still being calculated; in this
    * case we sleep 1 second and try again.
    * for this reason we do the operation in a sub-thread
    * @param f MxsObject representing the object to checksum
    * @param ec implicitly provided execution context
    * @return a Future, which resolves to a Try containing a String of the checksum.
    */
  def getOMFileMd5(f:MxsObject)(implicit ec:ExecutionContext):Future[Try[String]] = {
    val view = f.getAttributeView

    def lookup(attempt:Int=1):Try[String] = {
      val str = Try { view.readString("__mxs__calc_md5") }
      str match {
        case Success("")=>
          logger.info(s"Empty string returned for file MD5 on attempt $attempt, assuming still calculating. Will retry...")
          Thread.sleep(1000)  //this feels nasty but without resorting to actors i can't think of an elegant way
                                      //to delay and re-call in a non-blocking way
          lookup(attempt + 1)
        case err @ Failure(_)=>err
        case Success(result)=>
          val byteString = result.toArray.map(_.toByte)
          Success(convertBytesToHex(byteString))
      }
    }

    Future { lookup() }
  }
}
