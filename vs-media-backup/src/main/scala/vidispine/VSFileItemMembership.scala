package vidispine

import scala.util.{Failure, Success, Try}
import scala.xml.NodeSeq

case class VSFileItemMembership (itemId:String, shapes:Seq[VSFileShapeMembership])

case class VSFileShapeMembership(shapeId:String,componentId:Seq[String])

object VSFileItemMembership {
  def fromXml(node:NodeSeq):Try[VSFileItemMembership] = Try {
    new VSFileItemMembership(
      (node \ "id").text,
      (node \ "shape").map(shapeNode=>VSFileShapeMembership.fromXml(shapeNode) match {
        case Success(result)=>result
        case Failure(err)=>throw err
      })
    )
  }
}

object VSFileShapeMembership {
  def fromXml(node:NodeSeq):Try[VSFileShapeMembership] = Try {
    new VSFileShapeMembership(
      (node \ "id").text,
      (node \ "component").map(componentNode=>(componentNode \ "id").text)
    )
  }
}