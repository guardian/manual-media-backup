package models

import org.specs2.mutable.Specification

class PotentialArchiveTargetSpec extends Specification {

  "PotentialArchiveTarget.fromMediaCensusJson" should {
    "correctly decode sample data" in {
      val sampleRecord = """{"vsid":"KP-1172979","path":"crop_KP-1832.jpeg","uri":"omms://blah/blah/blah/crop_KP-1832.jpeg","state":"CLOSED","size":5300718,"hash":"bde41e82207a5cd3b322b3ae41ddfa6b9ed4bcef","timestamp":"2020-01-11T08:55:13.33Z","refreshFlag":1,"storage":"KP-2","metadata":{"MXFS_CREATIONDAY":"13","MXFS_MODIFICATION_TIME":"1415907286272","path":".","MXFS_CATEGORY":"5","MXFS_ARCHMONTH":"11","uuid":"1fc9181d-6b6c-11e4-8bcf-d2faaca98e3e-0","MXFS_PARENTOID":"","MXFS_ARCHYEAR":"2014","mtime":"1413211092000","MXFS_CREATION_TIME":"1415907286173","MXFS_ACCESS_TIME":"1545763578110","MXFS_ARCHDAY":"13","MXFS_FILENAME":"crop_KP-1832.jpeg","MXFS_ARCHIVE_TIME":"1415907286173","MXFS_CREATIONMONTH":"11","atime":"1415902315000","MXFS_CREATIONYEAR":"2014","MXFS_INTRASH":"false","created":"1413211092000"},"membership":{"itemId":"KP-1832","shapes":[{"shapeId":"KP-4426","componentId":["KP-14082","KP-14083"]},{"shapeId":"KP-4429","componentId":["KP-14082","KP-14083"]}]},"archiveHunterId":null,"archiveConflict":null}"""

      val result = PotentialArchiveTarget.fromMediaCensusJson(sampleRecord)
      result must beASuccessfulTry(PotentialArchiveTarget(
        Some(5300718L),
        Some("bde41e82207a5cd3b322b3ae41ddfa6b9ed4bcef"),
        "1fc9181d-6b6c-11e4-8bcf-d2faaca98e3e-0",
        "crop_KP-1832.jpeg",
        "KP-1172979",
        Some("KP-1832"),
        Some(Seq("KP-4426","KP-4429"))
      ))
    }

    "handle no membership case" in {
      val sampleRecord = """{"vsid":"KP-1172979","path":"crop_KP-1832.jpeg","uri":"omms://blah/blah/blah/crop_KP-1832.jpeg","state":"CLOSED","size":5300718,"hash":"bde41e82207a5cd3b322b3ae41ddfa6b9ed4bcef","timestamp":"2020-01-11T08:55:13.33Z","refreshFlag":1,"storage":"KP-2","metadata":{"MXFS_CREATIONDAY":"13","MXFS_MODIFICATION_TIME":"1415907286272","path":".","MXFS_CATEGORY":"5","MXFS_ARCHMONTH":"11","uuid":"1fc9181d-6b6c-11e4-8bcf-d2faaca98e3e-0","MXFS_PARENTOID":"","MXFS_ARCHYEAR":"2014","mtime":"1413211092000","MXFS_CREATION_TIME":"1415907286173","MXFS_ACCESS_TIME":"1545763578110","MXFS_ARCHDAY":"13","MXFS_FILENAME":"crop_KP-1832.jpeg","MXFS_ARCHIVE_TIME":"1415907286173","MXFS_CREATIONMONTH":"11","atime":"1415902315000","MXFS_CREATIONYEAR":"2014","MXFS_INTRASH":"false","created":"1413211092000"},"membership":null,"archiveHunterId":null,"archiveConflict":null}"""

      val result = PotentialArchiveTarget.fromMediaCensusJson(sampleRecord)
      result must beASuccessfulTry(PotentialArchiveTarget(
        Some(5300718L),
        Some("bde41e82207a5cd3b322b3ae41ddfa6b9ed4bcef"),
        "1fc9181d-6b6c-11e4-8bcf-d2faaca98e3e-0",
        "crop_KP-1832.jpeg",
        "KP-1172979",
        None,
        None
      ))
    }
  }
}
