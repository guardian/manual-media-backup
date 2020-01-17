case class Options (
                     vaultFile:String="",
                     localPath:String=sys.env.getOrElse("PWD", "")
                   )