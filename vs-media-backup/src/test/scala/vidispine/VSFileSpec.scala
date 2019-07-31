package vidispine

import java.time.ZonedDateTime

import org.specs2.mutable.Specification

class VSFileSpec extends Specification{
  "VSFile.storageSubpath" should {
    "strip the storage path out of a fullpath" in {
      val result = VSFile.storageSubpath("/path/to/storage/with/media/on/it","/path/to/storage")
      result must beSome("with/media/on/it")
    }

    "test what happens if they don't overlap" in {
      val result = VSFile.storageSubpath("/path/to/storage/with/media/on/it","/completely/different/path")
      result must beNone
    }
  }

  "VSFile.fromXml" should {
    "not fail if there is an empty string for file state" in {
      val sampleFileXml = """<file>
                            |            <id>VX-23948</id>
                            |            <path>EUBrusselsBrexitDebateRecording.mp4</path>
                            |            <uri>file:///srv/Proxies2/DevSystem/DAM/Scratch/EUBrusselsBrexitDebateRecording.mp4</uri>
                            |            <state></state>
                            |            <size>490896691</size>
                            |            <hash>af42e9eb15d4643238e990e7714492fc2fa0f8a7</hash>
                            |            <timestamp>2019-05-21T12:16:34.970+01:00</timestamp>
                            |            <refreshFlag>1</refreshFlag>
                            |            <storage>VX-18</storage>
                            |            <metadata>
                            |                <field>
                            |                    <key>created</key>
                            |                    <value>1558437337823</value>
                            |                </field>
                            |                <field>
                            |                    <key>mtime</key>
                            |                    <value>1558437337823</value>
                            |                </field>
                            |            </metadata>
                            |        </file>"""
      val result = VSFile.fromXmlString(sampleFileXml)
      result must beSuccessfulTry(Some(VSFile("VX-23948",
      "EUBrusselsBrexitDebateRecording.mp4",
      "file:///srv/Proxies2/DevSystem/DAM/Scratch/EUBrusselsBrexitDebateRecording.mp4",
        None,
        490896691L,
        Some("af42e9eb15d4643238e990e7714492fc2fa0f8a7"),
        ZonedDateTime.parse("2019-05-21T12:16:34.970+01:00"),
        1,
        "VX-18",
        Some(Map("created"->"1558437337823","mtime"->"1558437337823")),
        None
      )))
    }

    "not fail if an empty node is passed" in {
      val sampleFileXml = """<FileListDocument xmlns="http://xml.vidispine.com/schema/vidispine"/>"""
      val result = VSFile.fromXmlString(sampleFileXml)

      result must beSuccessfulTry(None)
    }
  }
}
