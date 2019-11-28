package vidispine

import java.time.ZonedDateTime

import org.specs2.mock.Mockito
import org.specs2.mutable.Specification
class VSShapeSpec extends Specification with Mockito {
  val sampleShapeXml = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
                         |<ShapeDocument xmlns="http://xml.vidispine.com/schema/vidispine">
                         |    <id>VX-6300</id>
                         |    <essenceVersion>1</essenceVersion>
                         |    <tag>original</tag>
                         |    <mimeType>video/mp4</mimeType>
                         |    <containerComponent>
                         |        <file>
                         |            <id>VX-23948</id>
                         |            <path>EUBrusselsBrexitDebateRecording.mp4</path>
                         |            <uri>file:///srv/Proxies2/DevSystem/DAM/Scratch/EUBrusselsBrexitDebateRecording.mp4</uri>
                         |            <state>CLOSED</state>
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
                         |        </file>
                         |        <id>VX-16670</id>
                         |        <metadata>
                         |            <key>creation_time</key>
                         |            <value>2019-01-22 14:56:11</value>
                         |        </metadata>
                         |        <metadata>
                         |            <key>XMP_METADATA</key>
                         |            <value>&lt;?xml version="1.0" encoding="UTF-8"?&gt;&lt;?xpacket begin="﻿" id="W5M0MpCehiHzreSzNTczkc9d"?&gt;&lt;x:xmpmeta xmlns:x="adobe:ns:meta/" x:xmptk="XMP Core 5.1.2"&gt;
                         |   &lt;rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"&gt;
                         |      &lt;rdf:Description xmlns:xmp="http://ns.adobe.com/xap/1.0/" rdf:about=""&gt;
                         |         &lt;xmp:CreateDate&gt;2019-01-22T14:56:11Z&lt;/xmp:CreateDate&gt;
                         |         &lt;xmp:ModifyDate&gt;2019-01-22T14:56:11Z&lt;/xmp:ModifyDate&gt;
                         |      &lt;/rdf:Description&gt;
                         |      &lt;rdf:Description xmlns:xmpDM="http://ns.adobe.com/xmp/1.0/DynamicMedia/" rdf:about=""&gt;
                         |         &lt;xmpDM:duration rdf:parseType="Resource"&gt;
                         |            &lt;xmpDM:value&gt;112935600&lt;/xmpDM:value&gt;
                         |            &lt;xmpDM:scale&gt;1/90000&lt;/xmpDM:scale&gt;
                         |         &lt;/xmpDM:duration&gt;
                         |      &lt;/rdf:Description&gt;
                         |   &lt;/rdf:RDF&gt;
                         |&lt;/x:xmpmeta&gt;&lt;?xpacket end="w"?&gt;</value>
                         |        </metadata>
                         |        <metadata>
                         |            <key>major_brand</key>
                         |            <value>f4v </value>
                         |        </metadata>
                         |        <metadata>
                         |            <key>compatible_brands</key>
                         |            <value>isommp42m4v </value>
                         |        </metadata>
                         |        <metadata>
                         |            <key>minor_version</key>
                         |            <value>0</value>
                         |        </metadata>
                         |        <duration>
                         |            <samples>1254840000</samples>
                         |            <timeBase>
                         |                <numerator>1</numerator>
                         |                <denominator>1000000</denominator>
                         |            </timeBase>
                         |        </duration>
                         |        <format>mov,mp4,m4a,3gp,3g2,mj2</format>
                         |        <startTimestamp>
                         |            <samples>0</samples>
                         |            <timeBase>
                         |                <numerator>1</numerator>
                         |                <denominator>90000</denominator>
                         |            </timeBase>
                         |        </startTimestamp>
                         |        <mediaInfo>
                         |            <property>
                         |                <key>Audio codecs</key>
                         |                <value>AAC LC</value>
                         |            </property>
                         |            <property>
                         |                <key>Audio_Format_List</key>
                         |                <value>AAC</value>
                         |            </property>
                         |            <property>
                         |                <key>Audio_Format_WithHint_List</key>
                         |                <value>AAC</value>
                         |            </property>
                         |            <property>
                         |                <key>Audio_Language_List</key>
                         |                <value>English</value>
                         |            </property>
                         |            <property>
                         |                <key>Codec</key>
                         |                <value>MPEG-4</value>
                         |            </property>
                         |            <property>
                         |                <key>Codec ID</key>
                         |                <value>f4v</value>
                         |            </property>
                         |            <property>
                         |                <key>Codec ID/Url</key>
                         |                <value>http://www.apple.com/quicktime/download/standalone.html</value>
                         |            </property>
                         |            <property>
                         |                <key>Codec/Extensions usually used</key>
                         |                <value>mp4 m4v m4a m4b m4p 3gpp 3gp 3gpp2 3g2 k3g jpm jpx mqv ismv isma f4v</value>
                         |            </property>
                         |            <property>
                         |                <key>Codecs Video</key>
                         |                <value>AVC</value>
                         |            </property>
                         |            <property>
                         |                <key>Commercial name</key>
                         |                <value>MPEG-4</value>
                         |            </property>
                         |            <property>
                         |                <key>Complete name</key>
                         |                <value>/srv/Proxies2/DevSystem/DAM/Scratch/EUBrusselsBrexitDebateRecording.mp4</value>
                         |            </property>
                         |            <property>
                         |                <key>Count</key>
                         |                <value>279</value>
                         |            </property>
                         |            <property>
                         |                <key>Count of audio streams</key>
                         |                <value>1</value>
                         |            </property>
                         |            <property>
                         |                <key>Count of stream of this kind</key>
                         |                <value>1</value>
                         |            </property>
                         |            <property>
                         |                <key>Count of video streams</key>
                         |                <value>1</value>
                         |            </property>
                         |            <property>
                         |                <key>DataSize</key>
                         |                <value>490479611</value>
                         |            </property>
                         |            <property>
                         |                <key>Duration</key>
                         |                <value>1254840</value>
                         |            </property>
                         |            <property>
                         |                <key>Encoded date</key>
                         |                <value>UTC 2019-01-22 14:56:11</value>
                         |            </property>
                         |            <property>
                         |                <key>File extension</key>
                         |                <value>mp4</value>
                         |            </property>
                         |            <property>
                         |                <key>File last modification date</key>
                         |                <value>UTC 2019-05-21 11:15:37</value>
                         |            </property>
                         |            <property>
                         |                <key>File last modification date (local)</key>
                         |                <value>2019-05-21 12:15:37</value>
                         |            </property>
                         |            <property>
                         |                <key>File name</key>
                         |                <value>EUBrusselsBrexitDebateRecording</value>
                         |            </property>
                         |            <property>
                         |                <key>File size</key>
                         |                <value>490896691</value>
                         |            </property>
                         |            <property>
                         |                <key>Folder name</key>
                         |                <value>/srv/Proxies2/DevSystem/DAM/Scratch</value>
                         |            </property>
                         |            <property>
                         |                <key>FooterSize</key>
                         |                <value>417044</value>
                         |            </property>
                         |            <property>
                         |                <key>Format</key>
                         |                <value>MPEG-4</value>
                         |            </property>
                         |            <property>
                         |                <key>Format profile</key>
                         |                <value>Adobe Flash</value>
                         |            </property>
                         |            <property>
                         |                <key>Format/Extensions usually used</key>
                         |                <value>mp4 m4v m4a m4b m4p 3gpp 3gp 3gpp2 3g2 k3g jpm jpx mqv ismv isma f4v</value>
                         |            </property>
                         |            <property>
                         |                <key>HeaderSize</key>
                         |                <value>36</value>
                         |            </property>
                         |            <property>
                         |                <key>Internet media type</key>
                         |                <value>video/mp4</value>
                         |            </property>
                         |            <property>
                         |                <key>IsStreamable</key>
                         |                <value>No</value>
                         |            </property>
                         |            <property>
                         |                <key>Kind of stream</key>
                         |                <value>General</value>
                         |            </property>
                         |            <property>
                         |                <key>Overall bit rate</key>
                         |                <value>3129621</value>
                         |            </property>
                         |            <property>
                         |                <key>Overall bit rate mode</key>
                         |                <value>VBR</value>
                         |            </property>
                         |            <property>
                         |                <key>Proportion of this stream</key>
                         |                <value>0.00085</value>
                         |            </property>
                         |            <property>
                         |                <key>Stream identifier</key>
                         |                <value>0</value>
                         |            </property>
                         |            <property>
                         |                <key>Stream size</key>
                         |                <value>417088</value>
                         |            </property>
                         |            <property>
                         |                <key>Tagged date</key>
                         |                <value>UTC 2019-01-22 14:56:11</value>
                         |            </property>
                         |            <property>
                         |                <key>Video_Format_List</key>
                         |                <value>AVC</value>
                         |            </property>
                         |            <property>
                         |                <key>Video_Format_WithHint_List</key>
                         |                <value>AVC</value>
                         |            </property>
                         |            <property>
                         |                <key>Video_Language_List</key>
                         |                <value>English</value>
                         |            </property>
                         |        </mediaInfo>
                         |    </containerComponent>
                         |    <audioComponent>
                         |        <file>
                         |            <id>VX-23948</id>
                         |            <path>EUBrusselsBrexitDebateRecording.mp4</path>
                         |            <uri>file:///srv/Proxies2/DevSystem/DAM/Scratch/EUBrusselsBrexitDebateRecording.mp4</uri>
                         |            <state>CLOSED</state>
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
                         |        </file>
                         |        <id>VX-16673</id>
                         |        <metadata>
                         |            <key>creation_time</key>
                         |            <value>2019-01-22 14:56:11</value>
                         |        </metadata>
                         |        <metadata>
                         |            <key>handler_name</key>
                         |            <value>owzaStreamingEngine</value>
                         |        </metadata>
                         |        <metadata>
                         |            <key>language</key>
                         |            <value>eng</value>
                         |        </metadata>
                         |        <codec>aac</codec>
                         |        <timeBase>
                         |            <numerator>1</numerator>
                         |            <denominator>48000</denominator>
                         |        </timeBase>
                         |        <itemTrack>A1</itemTrack>
                         |        <essenceStreamId>1</essenceStreamId>
                         |        <bitrate>125374</bitrate>
                         |        <numberOfPackets>58819</numberOfPackets>
                         |        <extradata>1190</extradata>
                         |        <pid>2</pid>
                         |        <duration>
                         |            <samples>112932480</samples>
                         |            <timeBase>
                         |                <numerator>1</numerator>
                         |                <denominator>90000</denominator>
                         |            </timeBase>
                         |        </duration>
                         |        <startTimestamp>
                         |            <samples>0</samples>
                         |            <timeBase>
                         |                <numerator>1</numerator>
                         |                <denominator>90000</denominator>
                         |            </timeBase>
                         |        </startTimestamp>
                         |        <channelCount>2</channelCount>
                         |        <channelLayout>3</channelLayout>
                         |        <sampleFormat>AV_SAMPLE_FMT_S16</sampleFormat>
                         |        <frameSize>1024</frameSize>
                         |        <mediaInfo>
                         |            <Bit_rate_mode>VBR</Bit_rate_mode>
                         |            <property>
                         |                <key>Bit rate</key>
                         |                <value>125373</value>
                         |            </property>
                         |            <property>
                         |                <key>Bit rate mode</key>
                         |                <value>VBR</value>
                         |            </property>
                         |            <property>
                         |                <key>Channel positions</key>
                         |                <value>2/0/0</value>
                         |            </property>
                         |            <property>
                         |                <key>Channel(s)</key>
                         |                <value>2</value>
                         |            </property>
                         |            <property>
                         |                <key>Codec</key>
                         |                <value>AAC LC</value>
                         |            </property>
                         |            <property>
                         |                <key>Codec ID</key>
                         |                <value>40</value>
                         |            </property>
                         |            <property>
                         |                <key>Codec/CC</key>
                         |                <value>40</value>
                         |            </property>
                         |            <property>
                         |                <key>Codec/Family</key>
                         |                <value>AAC</value>
                         |            </property>
                         |            <property>
                         |                <key>Commercial name</key>
                         |                <value>AAC</value>
                         |            </property>
                         |            <property>
                         |                <key>Compression mode</key>
                         |                <value>Lossy</value>
                         |            </property>
                         |            <property>
                         |                <key>Count</key>
                         |                <value>218</value>
                         |            </property>
                         |            <property>
                         |                <key>Count of stream of this kind</key>
                         |                <value>1</value>
                         |            </property>
                         |            <property>
                         |                <key>Delay</key>
                         |                <value>0</value>
                         |            </property>
                         |            <property>
                         |                <key>Delay relative to video</key>
                         |                <value>0</value>
                         |            </property>
                         |            <property>
                         |                <key>Delay, origin</key>
                         |                <value>Container</value>
                         |            </property>
                         |            <property>
                         |                <key>Duration</key>
                         |                <value>1254812</value>
                         |            </property>
                         |            <property>
                         |                <key>Encoded date</key>
                         |                <value>UTC 2019-01-22 14:56:11</value>
                         |            </property>
                         |            <property>
                         |                <key>Format</key>
                         |                <value>AAC</value>
                         |            </property>
                         |            <property>
                         |                <key>Format profile</key>
                         |                <value>LC</value>
                         |            </property>
                         |            <property>
                         |                <key>Format/Info</key>
                         |                <value>Advanced Audio Codec</value>
                         |            </property>
                         |            <property>
                         |                <key>Frame count</key>
                         |                <value>58819</value>
                         |            </property>
                         |            <property>
                         |                <key>ID</key>
                         |                <value>2</value>
                         |            </property>
                         |            <property>
                         |                <key>Kind of stream</key>
                         |                <value>Audio</value>
                         |            </property>
                         |            <property>
                         |                <key>Language</key>
                         |                <value>en</value>
                         |            </property>
                         |            <property>
                         |                <key>Proportion of this stream</key>
                         |                <value>0.04006</value>
                         |            </property>
                         |            <property>
                         |                <key>Samples count</key>
                         |                <value>60230976</value>
                         |            </property>
                         |            <property>
                         |                <key>Sampling rate</key>
                         |                <value>48000</value>
                         |            </property>
                         |            <property>
                         |                <key>Source duration</key>
                         |                <value>1254805</value>
                         |            </property>
                         |            <property>
                         |                <key>Source frame count</key>
                         |                <value>58819</value>
                         |            </property>
                         |            <property>
                         |                <key>Source stream size</key>
                         |                <value>19665002</value>
                         |            </property>
                         |            <property>
                         |                <key>Source_StreamSize_Proportion</key>
                         |                <value>0.04006</value>
                         |            </property>
                         |            <property>
                         |                <key>Stream identifier</key>
                         |                <value>0</value>
                         |            </property>
                         |            <property>
                         |                <key>Stream size</key>
                         |                <value>19665002</value>
                         |            </property>
                         |            <property>
                         |                <key>StreamOrder</key>
                         |                <value>1</value>
                         |            </property>
                         |            <property>
                         |                <key>Tagged date</key>
                         |                <value>UTC 2019-01-22 14:56:11</value>
                         |            </property>
                         |            <property>
                         |                <key>Title</key>
                         |                <value>WowzaStreamingEngine</value>
                         |            </property>
                         |            <property>
                         |                <key>Video0 delay</key>
                         |                <value>0</value>
                         |            </property>
                         |            <property>
                         |                <key>mdhd_Duration</key>
                         |                <value>1254812</value>
                         |            </property>
                         |        </mediaInfo>
                         |    </audioComponent>
                         |    <videoComponent>
                         |        <file>
                         |            <id>VX-23948</id>
                         |            <path>EUBrusselsBrexitDebateRecording.mp4</path>
                         |            <uri>file:///srv/Proxies2/DevSystem/DAM/Scratch/EUBrusselsBrexitDebateRecording.mp4</uri>
                         |            <state>CLOSED</state>
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
                         |        </file>
                         |        <id>VX-16674</id>
                         |        <metadata>
                         |            <key>creation_time</key>
                         |            <value>2019-01-22 14:56:11</value>
                         |        </metadata>
                         |        <metadata>
                         |            <key>handler_name</key>
                         |            <value>owzaStreamingEngine</value>
                         |        </metadata>
                         |        <metadata>
                         |            <key>language</key>
                         |            <value>eng</value>
                         |        </metadata>
                         |        <codec>h264</codec>
                         |        <timeBase>
                         |            <numerator>1</numerator>
                         |            <denominator>90000</denominator>
                         |        </timeBase>
                         |        <itemTrack>V1</itemTrack>
                         |        <essenceStreamId>0</essenceStreamId>
                         |        <bitrate>3001591</bitrate>
                         |        <numberOfPackets>31371</numberOfPackets>
                         |        <extradata>01640028FFE1002567640028AC2B402802DD808800001F4000061A87000003016E360000B71B3779707C70CA8001000568EE3CB000</extradata>
                         |        <pid>1</pid>
                         |        <duration>
                         |            <samples>112935600</samples>
                         |            <timeBase>
                         |                <numerator>1</numerator>
                         |                <denominator>90000</denominator>
                         |            </timeBase>
                         |        </duration>
                         |        <profile>100</profile>
                         |        <level>40</level>
                         |        <startTimestamp>
                         |            <samples>0</samples>
                         |            <timeBase>
                         |                <numerator>1</numerator>
                         |                <denominator>90000</denominator>
                         |            </timeBase>
                         |        </startTimestamp>
                         |        <resolution>
                         |            <width>1280</width>
                         |            <height>720</height>
                         |        </resolution>
                         |        <pixelFormat>yuv420p</pixelFormat>
                         |        <maxBFrames>0</maxBFrames>
                         |        <pixelAspectRatio>
                         |            <horizontal>1</horizontal>
                         |            <vertical>1</vertical>
                         |        </pixelAspectRatio>
                         |        <fieldOrder>progressive</fieldOrder>
                         |        <codecTimeBase>
                         |            <numerator>1</numerator>
                         |            <denominator>50</denominator>
                         |        </codecTimeBase>
                         |        <averageFrameRate>
                         |            <numerator>25</numerator>
                         |            <denominator>1</denominator>
                         |        </averageFrameRate>
                         |        <realBaseFrameRate>
                         |            <numerator>25</numerator>
                         |            <denominator>1</denominator>
                         |        </realBaseFrameRate>
                         |        <displayWidth>
                         |            <numerator>1280</numerator>
                         |            <denominator>1</denominator>
                         |        </displayWidth>
                         |        <displayHeight>
                         |            <numerator>720</numerator>
                         |            <denominator>1</denominator>
                         |        </displayHeight>
                         |        <max_packet_size>154110</max_packet_size>
                         |        <ticks_per_frame>2</ticks_per_frame>
                         |        <bitDepth>8</bitDepth>
                         |        <bitsPerPixel>12</bitsPerPixel>
                         |        <mediaInfo>
                         |            <Bit_rate_mode>CBR</Bit_rate_mode>
                         |            <property>
                         |                <key>Bit depth</key>
                         |                <value>8</value>
                         |            </property>
                         |            <property>
                         |                <key>Bit rate</key>
                         |                <value>3000000</value>
                         |            </property>
                         |            <property>
                         |                <key>Bit rate mode</key>
                         |                <value>CBR</value>
                         |            </property>
                         |            <property>
                         |                <key>Bits/(Pixel*Frame)</key>
                         |                <value>0.130</value>
                         |            </property>
                         |            <property>
                         |                <key>Buffer size</key>
                         |                <value>3000000</value>
                         |            </property>
                         |            <property>
                         |                <key>Chroma subsampling</key>
                         |                <value>4:2:0</value>
                         |            </property>
                         |            <property>
                         |                <key>Codec</key>
                         |                <value>AVC</value>
                         |            </property>
                         |            <property>
                         |                <key>Codec ID</key>
                         |                <value>avc1</value>
                         |            </property>
                         |            <property>
                         |                <key>Codec ID/Info</key>
                         |                <value>Advanced Video Coding</value>
                         |            </property>
                         |            <property>
                         |                <key>Codec ID/Url</key>
                         |                <value>http://www.apple.com/quicktime/download/standalone.html</value>
                         |            </property>
                         |            <property>
                         |                <key>Codec profile</key>
                         |                <value>High@L4.0</value>
                         |            </property>
                         |            <property>
                         |                <key>Codec settings</key>
                         |                <value>CABAC / 1 Ref Frames</value>
                         |            </property>
                         |            <property>
                         |                <key>Codec settings, CABAC</key>
                         |                <value>Yes</value>
                         |            </property>
                         |            <property>
                         |                <key>Codec/CC</key>
                         |                <value>avc1</value>
                         |            </property>
                         |            <property>
                         |                <key>Codec/Family</key>
                         |                <value>AVC</value>
                         |            </property>
                         |            <property>
                         |                <key>Codec/Info</key>
                         |                <value>Advanced Video Codec</value>
                         |            </property>
                         |            <property>
                         |                <key>Codec/Url</key>
                         |                <value>http://developers.videolan.org/x264.html</value>
                         |            </property>
                         |            <property>
                         |                <key>Codec_Settings_RefFrames</key>
                         |                <value>1</value>
                         |            </property>
                         |            <property>
                         |                <key>Color space</key>
                         |                <value>YUV</value>
                         |            </property>
                         |            <property>
                         |                <key>Colorimetry</key>
                         |                <value>4:2:0</value>
                         |            </property>
                         |            <property>
                         |                <key>Commercial name</key>
                         |                <value>AVC</value>
                         |            </property>
                         |            <property>
                         |                <key>Count</key>
                         |                <value>248</value>
                         |            </property>
                         |            <property>
                         |                <key>Count of stream of this kind</key>
                         |                <value>1</value>
                         |            </property>
                         |            <property>
                         |                <key>Delay</key>
                         |                <value>0</value>
                         |            </property>
                         |            <property>
                         |                <key>Delay, origin</key>
                         |                <value>Container</value>
                         |            </property>
                         |            <property>
                         |                <key>Display aspect ratio</key>
                         |                <value>16:9</value>
                         |            </property>
                         |            <property>
                         |                <key>Duration</key>
                         |                <value>1254840</value>
                         |            </property>
                         |            <property>
                         |                <key>Encoded date</key>
                         |                <value>UTC 2019-01-22 14:56:11</value>
                         |            </property>
                         |            <property>
                         |                <key>Format</key>
                         |                <value>AVC</value>
                         |            </property>
                         |            <property>
                         |                <key>Format profile</key>
                         |                <value>High@L4.0</value>
                         |            </property>
                         |            <property>
                         |                <key>Format settings</key>
                         |                <value>CABAC / 1 Ref Frames</value>
                         |            </property>
                         |            <property>
                         |                <key>Format settings, CABAC</key>
                         |                <value>Yes</value>
                         |            </property>
                         |            <property>
                         |                <key>Format settings, GOP</key>
                         |                <value>M=1, N=25</value>
                         |            </property>
                         |            <property>
                         |                <key>Format settings, ReFrames</key>
                         |                <value>1</value>
                         |            </property>
                         |            <property>
                         |                <key>Format/Info</key>
                         |                <value>Advanced Video Codec</value>
                         |            </property>
                         |            <property>
                         |                <key>Format/Url</key>
                         |                <value>http://developers.videolan.org/x264.html</value>
                         |            </property>
                         |            <property>
                         |                <key>Frame count</key>
                         |                <value>31371</value>
                         |            </property>
                         |            <property>
                         |                <key>Frame rate</key>
                         |                <value>25.000</value>
                         |            </property>
                         |            <property>
                         |                <key>Frame rate mode</key>
                         |                <value>CFR</value>
                         |            </property>
                         |            <property>
                         |                <key>Height</key>
                         |                <value>720</value>
                         |            </property>
                         |            <property>
                         |                <key>ID</key>
                         |                <value>1</value>
                         |            </property>
                         |            <property>
                         |                <key>Interlacement</key>
                         |                <value>PPF</value>
                         |            </property>
                         |            <property>
                         |                <key>Internet media type</key>
                         |                <value>video/H264</value>
                         |            </property>
                         |            <property>
                         |                <key>Kind of stream</key>
                         |                <value>Video</value>
                         |            </property>
                         |            <property>
                         |                <key>Language</key>
                         |                <value>en</value>
                         |            </property>
                         |            <property>
                         |                <key>Pixel aspect ratio</key>
                         |                <value>1.000</value>
                         |            </property>
                         |            <property>
                         |                <key>Proportion of this stream</key>
                         |                <value>0.95909</value>
                         |            </property>
                         |            <property>
                         |                <key>Resolution</key>
                         |                <value>8</value>
                         |            </property>
                         |            <property>
                         |                <key>Rotation</key>
                         |                <value>0.000</value>
                         |            </property>
                         |            <property>
                         |                <key>Scan type</key>
                         |                <value>Progressive</value>
                         |            </property>
                         |            <property>
                         |                <key>Stream identifier</key>
                         |                <value>0</value>
                         |            </property>
                         |            <property>
                         |                <key>Stream size</key>
                         |                <value>470814601</value>
                         |            </property>
                         |            <property>
                         |                <key>StreamOrder</key>
                         |                <value>0</value>
                         |            </property>
                         |            <property>
                         |                <key>Tagged date</key>
                         |                <value>UTC 2019-01-22 14:56:11</value>
                         |            </property>
                         |            <property>
                         |                <key>Title</key>
                         |                <value>WowzaStreamingEngine</value>
                         |            </property>
                         |            <property>
                         |                <key>Width</key>
                         |                <value>1280</value>
                         |            </property>
                         |            <Format_Settings_GOP>M=1, N=25</Format_Settings_GOP>
                         |        </mediaInfo>
                         |    </videoComponent>
                         |    <metadata>
                         |        <field>
                         |            <key>originalFilename</key>
                         |            <value>EUBrusselsBrexitDebateRecording.mp4</value>
                         |        </field>
                         |    </metadata>
                         |</ShapeDocument>""".stripMargin

  val sampleUriListXml = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
                           |<URIListDocument xmlns="http://xml.vidispine.com/schema/vidispine">
                           |    <uri>VX-6300</uri>
                           |</URIListDocument>""".stripMargin

  "VSShape.fromXml" should {
    "parse a VS shape XML and return domain objects" in {
      val result = VSShape.fromXmlString(sampleShapeXml)

      result must beSuccessfulTry(VSShape("VX-6300", 1, "original", "video/mp4",
        Seq(
          VSFile("VX-23948","EUBrusselsBrexitDebateRecording.mp4","file:///srv/Proxies2/DevSystem/DAM/Scratch/EUBrusselsBrexitDebateRecording.mp4",
            Some(VSFileState.CLOSED),490896691L,Some("af42e9eb15d4643238e990e7714492fc2fa0f8a7"),
            ZonedDateTime.parse("2019-05-21T12:16:34.970+01:00"),1,"VX-18",Some(Map("created" -> "1558437337823", "mtime" -> "1558437337823")),None
          ),
          VSFile("VX-23948","EUBrusselsBrexitDebateRecording.mp4","file:///srv/Proxies2/DevSystem/DAM/Scratch/EUBrusselsBrexitDebateRecording.mp4",
            Some(VSFileState.CLOSED),490896691,Some("af42e9eb15d4643238e990e7714492fc2fa0f8a7"),
            ZonedDateTime.parse("2019-05-21T12:16:34.970+01:00"),1,"VX-18",Some(Map("created" -> "1558437337823", "mtime" -> "1558437337823")),None),
          VSFile("VX-23948","EUBrusselsBrexitDebateRecording.mp4","file:///srv/Proxies2/DevSystem/DAM/Scratch/EUBrusselsBrexitDebateRecording.mp4",
            Some(VSFileState.CLOSED),490896691,Some("af42e9eb15d4643238e990e7714492fc2fa0f8a7"),
            ZonedDateTime.parse("2019-05-21T12:16:34.970+01:00"),1,"VX-18",Some(Map("created" -> "1558437337823", "mtime" -> "1558437337823")),None)
        )
      ))
    }
  }

//  "VSShape.vsIdForShapeTag" should {
//    "return the provided VSID" in {
//      val mockedCommunicator = mock[VSCommunicator]
//      mockedCommunicator.requestGet(any,any)(any,any,any) returns Future(Right(sampleUriListXml))
//
//      VSShape.vsIdForShapeTag("VX-1234","original")(mockedCommunicator, )
//    }
//  }
}
