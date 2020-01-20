case class Options (
                     vaultFile:String="",
                     localPath:String=sys.env.getOrElse("PWD", ""),
                     excludePathsFile:Option[String]=None,
                     reportOutputFile:Option[String]=None,
                     matchParallel:Int=10
                   )