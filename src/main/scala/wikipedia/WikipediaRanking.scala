package wikipedia

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

case class WikipediaArticle(title: String, text: String)

object WikipediaRanking {

  val langs = List(
    "JavaScript", "Java", "PHP", "Python", "c#", "C++", "Ruby", "CSS",
    "Objective-C", "Perl", "Scala", "Haskell", "MATLAB", "Clojure", "Groovy"
  )

  val spark = SparkSession.builder()
      .master("local[*]")
      .appName("wikipedia")
      .getOrCreate()

  val sc = spark.sparkContext

  // store rdd in memory
  val wikiRDD: RDD[WikipediaArticle] =
    sc.textFile(WikipediaData.filePath).map(WikipediaData.parse).persist()

  // a needle in a haystack＝干し草の中から針を一本探す
  // (多くな中から本当に小さなものをさがす)、見つけるのは至難の業
  private def textContains(needle: String, haystack: String): Boolean =
    haystack.split(" ").contains(needle)

  private def findLanguages(langs: List[String], article: WikipediaArticle) =
    langs.filter(textContains(_, article.text))

  // 言語の出現回数
  def occurrencesOfLang(lang: String, rdd: RDD[WikipediaArticle]): Int =
    rdd
        .filter(
          article => textContains(lang, article.text)
        )
        .count()
        .toInt

  def rankLangs(langs: List[String], rdd: RDD[WikipediaArticle]): List[(String, Int)] =
    langs
        .map(lang => (lang, occurrencesOfLang(lang, rdd)))
        // -で降順にする
        .sortBy(-_._2)

  def makeIndex(langs: List[String], rdd: RDD[WikipediaArticle])
  : RDD[(String, Iterable[WikipediaArticle])] =
    rdd.flatMap(article => {
      findLanguages(langs, article)
          .map(lang => (lang, article))
    }).groupByKey

  def rankLangsUsingIndex(index: RDD[(String, Iterable[WikipediaArticle])]): List[(String, Int)] =
    index
        .map({
          case (lang, articles) => (lang, articles.size)
        })
        .sortBy(-_._2)
        .collect()
        .toList

  def rankLangsReduceByKey(langs: List[String], rdd: RDD[WikipediaArticle]): List[(String, Int)] =
    rdd
        .flatMap(article => {
          findLanguages(langs, article)
              .map(lang => (lang, 1))
        })
        .reduceByKey(_ + _)
        .sortBy(-_._2)
        .collect()
        .toList

  def main(args: Array[String]): Unit = {
    val langsRanked: List[(String, Int)] =
      timed("Part 1: naive ranking", rankLangs(langs, wikiRDD))

    def index: RDD[(String, Iterable[WikipediaArticle])] = makeIndex(langs, wikiRDD)

    val langsRanked2: List[(String, Int)] =
      timed("Part 2: ranking using inverted index", rankLangsUsingIndex(index))

    val langsRanked3: List[(String, Int)] =
      timed("Part 3: ranking using reduceByKey", rankLangsReduceByKey(langs, wikiRDD))

    println(timing)
    sc.stop()
  }

  val timing = new StringBuffer

  def timed[T](label: String, code: => T): T = {
    val start = System.currentTimeMillis()
    val result = code
    val stop = System.currentTimeMillis()
    timing.append(s"Processing $label took ${stop - start} ms.\n")
    result
  }

}
