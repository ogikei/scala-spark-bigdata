package wikipedia

import java.io.File

object WikipediaData {

  private[wikipedia] def filePath = {
    val resource = this.getClass.getClassLoader.getResource("wikipedia/wikipedia.dat")
    if (resource == null)
      sys.error("Please download the dataset as explained in the assignment instructions")
    new File(resource.toURI).getPath
  }

  private[wikipedia] def parse(line: String): WikipediaArticle = {
    val subs = "</title><text>"
    // "abcabc".indexOf("bc")
    // => 1
    val i = line.indexOf(subs)
    // var str = "HELLO"
    // println("" + str.substring(0,2)) => HE
    // println("" + str.substring(3,5)) => LO
    // println("" + str.substring(1,4)) => ELL
    val title = line.substring(14, i)
    val text = line.substring(i + subs.length, line.length - 16)
    WikipediaArticle(title, text)
  }

}
