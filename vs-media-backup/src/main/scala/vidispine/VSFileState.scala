package vidispine

import io.circe.{Decoder, Encoder}

//see http://apidoc.vidispine.com/latest/storage/storage.html?highlight=lost#file-states
object VSFileState extends Enumeration {
  type VSFileState = Value
  val NONE,OPEN,CLOSED,UNKNOWN,MISSING,LOST,TO_APPEAR,TO_BE_DELETED,BEING_READ,ARCHIVED,AWAITING_SYNC = Value
}

trait VSFileStateEncoder {
  implicit val fileStateEncoder = Encoder.enumEncoder(VSFileState)
  implicit val fileStateDecoder = Decoder.enumDecoder(VSFileState)
}