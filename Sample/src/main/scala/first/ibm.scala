package first






import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
object ibm{

  def main(args: Array[String]) {
   val conf = new SparkConf()
      .setAppName("Count")
      .setMaster("local")
    val sc = new SparkContext(conf)


    val df = sc.textFile("/Users/gouthamrajanala/Downloads/text.txt")
    //("")

    // Displays the content of the DataFrame to stdout
    val d = df.map(x=>x.split(" "))
    d.collect().mkString.foreach(println)
    //println(d)
//    val rdd = sc.parallelize(list1)

    //rdd.collect().foreach(println(_))
  }



  }


/*
object new{
  def main(args: Array[String]) {
    val KeytoValue = Map(
      1 -> ds.printSchema(),
      2 -> ds.show(),
      3 -> customerDF.show(),
      4 -> customerDF.show(2),
      5 -> customerDF.describe("sales", "discount").show(),
      6 -> customerDF.printSchema(),
      7 -> allCust.show(),
      8 -> californiaCust.show(),
      //9 ->  caseInsensitive.show(),

    )
    val list = List("1. Dataset printschema", "2. Show Dataset", "3. See the DataFrame contents", "4. See the first few lines of the DataFrame contents", "5. Statistics for the numerical columns", "6. A DataFrame has a schema", "7. Very simple query on Customer table", "8. Very simple query with a filter", "9. Select all from table\n")
    list.foreach(println(_))
    println("Select the option\n")
    val key = KeytoValue(scala.io.StdIn.readInt())
    println(key)



*/


