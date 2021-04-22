package models

import org.specs2.mutable.Specification

import java.nio.file.Paths

class PathTransformSpec extends Specification {
  "PathTransform.fromPathSpec" should {
    "create a PathTransform object with correct from and to parts" in {
      val result = PathTransform.fromPathSpec("/old/path/to/replace=/new/path/replaced")
      result must beRight

      result.right.get.from.toString mustEqual "/old/path/to/replace"
      result.right.get.to.toString mustEqual "/new/path/replaced"
    }
  }

  "PathTransform.apply" should {
    "change the base path for a media file" in {
      val mediaPath = Paths.get("/old/path/to/replace/wg/comm/project/media/somefile.mxf")
      val transform = PathTransform("/old/path/to/replace","/new/path/replaced")
      val result = transform.apply(mediaPath)

      result must beSuccessfulTry
      result.get.toString mustEqual "/new/path/replaced/wg/comm/project/media/somefile.mxf"
    }

    "not work if the media file comes from a different base path" in {
      val mediaPath = Paths.get("/random/broken/path/media/somefile.mxf")
      val transform = PathTransform("/old/path/to/replace","/new/path/replaced")
      val result = transform.apply(mediaPath)

      result must beFailedTry
    }
  }
}
