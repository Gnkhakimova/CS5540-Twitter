
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}
object queries_phse2 {
  def main(args: Array[String]) {


    val sparkConf = new SparkConf().setAppName("Bitcoin").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val tweet = sqlContext.read.json("/Volumes/Data/PrinciplesOfBigData/Phase2Bitcoin/src/main/resources/tweets_output.json")
    tweet.createOrReplaceTempView("Bitcoin")


    println("---------------------------------------")
    println("PHASE 2 - PRINCIPLES OF BIG DATA PROJECT")
    println("---------------------------------------")
    println("Enter any one of the following query to get corresponding data ")
    println("1.Query 1 : Bitcoin passionate people and what they say.. ")
    println("2.Query 2 : Total tweets from different time zones")
    println("3.Query 3 : Verified Users")
    println("4.Query 4 List screen name based on conditions - 'olympics' in text and screen name starts and ends with 'm':")
    println("Enter any one of the following query to get data:")
    val count = scala.io.StdIn.readLine()

    count match {
      case "1" =>

        val query_1 = sqlContext.sql("SELECT user.name, text FROM Bitcoin WHERE text LIKE '%bitcoin%'")
        query_1.show()

      case "2" =>
        val query_4 = sqlContext.sql("SELECT user.time_zone,count(text) AS Total FROM Bitcoin WHERE user.time_zone IS NOT NULL GROUP BY user.time_zone ORDER BY Total DESC LIMIT 10")
        query_4.show()


      case "3" =>
        val query_8 = sqlContext.sql("SELECT count(user.verified) AS Verified_Users FROM Bitcoin WHERE user.verified=true")
        query_8.show()

      case "4" =>
        val query_10 = sqlContext.sql("SELECT user.screen_name, user.followers_count,user.favourites_count, user.friends_count FROM Bitcoin WHERE user.screen_name LIKE 'm%' AND text LIKE '%olympics%'")
        query_10.show()

    }
  }
}

