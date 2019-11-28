package vidispine

import scala.util.{Failure, Success, Try}
import scala.xml.{Node, XML}

/**
  * this class does not attempt to model the entire VS Storage dataset, just the bits that we need
  */
case class VSStorage (vsid:String, storageState:String, methods:Seq[VSStorageMethod]) {
  def fileSystemMethods = {
    methods.filter(_.uri.scheme=="file")
  }

  def fileSystemUris = fileSystemMethods.map(_.uri)

  def fileSystemPaths = fileSystemUris.map(_.path.mkString("/"))
}

object VSStorage {
  def fromXmlString(xmlString:String) = fromXml(XML.loadString(xmlString))

  def fromXml(elem: Node):Try[VSStorage] = Try {
    new VSStorage(
      (elem \ "id").text,
      (elem \ "state").text,
      (elem \ "method").map(methodNode=>VSStorageMethod.fromXml(methodNode) match {
        case Success(method)=>method
        case Failure(err)=>throw err  //immediately caught by surrounding Try and returned as a Failure
      })
    )
  }
}