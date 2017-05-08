import org.apache.spark.sql.Column

val columnNames = List("t010101", "t033400", "t118010011", "t100110", "t141555")

val categoryMappings = List(
  List("t01", "t03", "t11", "t1801", "t1803"),
  List("t05", "t1805"),
  List("t02", "t04", "t06", "t07", "t08", "t09", "t10", "t12", "t13", "t14", "t15", "t16", "t18")
).zipWithIndex

val groups: Map[Int, List[Column]] = columnNames
  .foldLeft(List.empty[(Int, Column)])((acc, name) => {
    categoryMappings
      .flatMap {
        case (prefixes, index) if prefixes.exists(name.startsWith) => Some((index, new Column(name)))
        case _ => None
      }
      .sortBy(_._1)
      .headOption match {
      case Some(tuple) => tuple :: acc
      case None => acc
    }
  })
  .groupBy(_._1)
  .mapValues(_.map(_._2))
