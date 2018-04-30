import java.lang.System.setProperty
import java.lang.System._

import org.apache
import scala.collection.JavaConversions._
import scala.collection.convert.wrapAll._
import scala.collection.convert.decorateAll._
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}



object Tweets {

  def main(args: Array[String]) {

    println("WELCOME")
    setProperty("hadoop.home.dir", "c:\\winutils\\")

    val conf = new SparkConf().setAppName("SparkSQL").setMaster("local").set("com.spark.executor", "")

    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)

    val jsonFile = sqlContext.jsonFile("/home/gulnoza/Desktop/tweets_output.json")

    jsonFile.registerTempTable("TweetTable")

    val querie1= sqlContext.sql("SELECT Count(id) AS Total, user.location AS Location FROM TweetTable WHERE user.location IS NOT NULL GROUP BY user.location ORDER BY COUNT(id) DESC")

    querie1.show()
    querie1.coalesce(1).write.format("com.databricks.spark.csv").option("header","true").save("/home/gulnoza/Desktop/out1.csv")
    //querie1.write.option("header","true").csv("/home/gulnoza/Desktop/out1.csv")

    val querie2= sqlContext.sql("Select user.name as Name, user.statuses_count as Statuses from TweetTable where text like '%forbes%' and user.verified=true order By user.statuses_count desc  ")

    querie2.show();
    querie2.coalesce(1).write.format("com.databricks.spark.csv").option("header","true").save("/home/gulnoza/Desktop/out2.csv")
    //querie2.write.option("header","true").csv("/home/gulnoza/Desktop/out2.csv")

    val querie3= sqlContext.sql("Select sum(user.followers_count) as Followers, user.lang as Language from TweetTable where retweeted_status.text like '%Olympics%' Group By user.lang Order BY sum(user.followers_count) desc")
    querie3.show();
    querie3.coalesce(1).write.format("com.databricks.spark.csv").option("header","true").save("/home/gulnoza/Desktop/out3.csv")
    //querie3.write.option("header","true").csv("/home/gulnoza/Desktop/out3.csv")

  }

}


