case class Options (lookup:Option[String]=None,
                    vaultFile:String="",
                    copyToLocal:Option[String]=None,
                    copyFromLocal:Option[String]=None,
                    chunkSize:Int=2,
                   checksumType:String="md5")