package wikipedia

import org.junit.runner.RunWith
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class WikipediaSuite extends FunSuite with BeforeAndAfterAll {

  def initializeWikipediaRanking(): Boolean =
    try {
      WikipediaRanking
      true
    } catch {
      // Throwableクラスはすべてのエラー、例外のスーパークラス
      case ex: Throwable =>
        println(ex.getMessage)
        ex.printStackTrace()
        false
    }

  override def afterAll(): Unit = {
    // initializeWikipediaRanking()がfalseの場合、後ろのメッセージが表示される。
    assert(initializeWikipediaRanking(),
      " -- did you fill in all the values in WikipediaRanking (conf, sc, wikiRdd)?")
    import WikipediaRanking._
    sc.stop()
  }


  def equivalentAndOrdered(given: List[(String, Int)], expected: List[(String, Int)]): Boolean = {
    // 言語の状態が同じ要素を含んでいるか
    (given.toSet == expected.toSet) &&
    // List(1,2,3,4) zip List('a,'b,'c)
    // => List((1,'a), (2,'b), (3,'c))
    // List[((String, Int), (String, Int))]
    // オーダーされているかを確認する
    !(given zip given.tail).exists({
      case ((_, occ1), (_, occ2)) => occ1 < occ2
    })
  }

  test("'occurrencesOfLang' should work for (specific) RDD with one element") {
    assert(initializeWikipediaRanking(),
      " -- did you fill in all the values in WikipediaRanking (conf, sc, wikiRdd)?")
    import WikipediaRanking._
    val rdd = sc.parallelize(Seq(WikipediaArticle("title", "Java Jakarta")))
    val res = occurrencesOfLang("Java", rdd) == 1
    assert(res, "occurrencesOfLang given (specific) RDD with one element should equal to 1")
  }

  test("'rankLangs' should work for RDD with two elements") {
    assert(initializeWikipediaRanking(),
      " -- did you fill in all the values in WikipediaRanking (conf, sc, wikiRdd)?")
    import WikipediaRanking._
    val langs = List("Scala", "Java")
    val rdd = sc.parallelize(List(
      WikipediaArticle("1", "Scala is great"),
      WikipediaArticle("2", "Java is OK, but Scala is cooler")))
    val ranked = rankLangs(langs, rdd)
    val res = ranked.head._1 == "Scala"
    assert(res)
  }

  test("'makeIndex' creates a simple index with two entries") {
    assert(initializeWikipediaRanking(),
      " -- did you fill in all the values in WikipediaRanking (conf, sc, wikiRdd)?")
    import WikipediaRanking._
    val langs = List("Scala", "Java")
    val articles = List(
      WikipediaArticle("1","Groovy is pretty interesting, and so is Erlang"),
      WikipediaArticle("2","Scala and Java run on the JVM"),
      WikipediaArticle("3","Scala is not purely functional")
    )
    val rdd = sc.parallelize(articles)
    val index = makeIndex(langs, rdd)
    val ranked = rankLangsUsingIndex(index)
    val res = ranked.head._1 == "Scala"
    assert(res)
  }

  test("'rankLangsReduceByKey' should work for a simple RDD with four elements") {
    assert(initializeWikipediaRanking(),
      " -- did you fill in all the values in WikipediaRanking (conf, sc, wikiRdd)?")
    import WikipediaRanking._
    val langs = List("Scala", "Java", "Groovy", "Haskell", "Erlang")
    val articles = List(
      WikipediaArticle("1","Groovy is pretty interesting, and so is Erlang"),
      WikipediaArticle("2","Scala and Java run on the JVM"),
      WikipediaArticle("3","Scala is not purely functional"),
      WikipediaArticle("4","The cool kids like Haskell more than Java"),
      WikipediaArticle("5","Java is for enterprise developers")
    )
    val rdd = sc.parallelize(articles)
    val ranked = rankLangsReduceByKey(langs, rdd)
    val res = ranked.head._1 == "Java"
    assert(res)
  }

}
