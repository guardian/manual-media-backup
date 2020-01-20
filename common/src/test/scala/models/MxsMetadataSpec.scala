package models

import org.specs2.mutable.Specification

class MxsMetadataSpec extends Specification {
  "MxsMetadata.toAttributes" should {
    "turn a data structure into an attribute list" in {
      val meta = MxsMetadata.empty().withValue("stringone","valueone").withValue("intone",1).withValue("__mxs_something",999L).withValue("__mxs_something_else","sdfsdfs")

      val result = meta.toAttributes()
      result.head.getKey mustEqual "stringone"
      result.head.getValue mustEqual "valueone"
      result(1).getKey mustEqual "__mxs_something_else"
      result(1).getValue mustEqual "sdfsdfs"
      result.length mustEqual 4
    }

    "not include any __mxs attributes if filterUnwritable is true" in {
      val meta = MxsMetadata.empty().withValue("stringone","valueone").withValue("intone",1).withValue("__mxs_something",999L).withValue("__mxs_something_else","sdfsdfs")

      val result = meta.toAttributes(filterUnwritable = true)
      result.head.getKey mustEqual "stringone"
      result.head.getValue mustEqual "valueone"
      result(1).getKey mustEqual "intone"
      result(1).getValue mustEqual 1
      result.length mustEqual 2

    }
  }
}
