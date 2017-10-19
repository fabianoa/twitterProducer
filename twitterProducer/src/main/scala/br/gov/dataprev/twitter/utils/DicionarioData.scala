package br.gov.dataprev.twitter.utils

import java.io.File

object DicionarioData {
  
   def filePath(dicionario:String ) = {

    val resource = this.getClass.getClassLoader.getResource(dicionario)

    if (resource == null) sys.error("Please download the dataset as explained in the assignment instructions")

    new File(resource.toURI).getPath

  }



  
}