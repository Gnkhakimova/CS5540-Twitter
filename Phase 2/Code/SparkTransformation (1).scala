import java.lang.System.setProperty
import java.lang.System._

import scala.collection.JavaConversions._
import scala.collection.convert.wrapAll._
import scala.collection.convert.decorateAll._
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}


object SparkTransformation {
  def main(args: Array[String]) {


    setProperty("hadoop.home.dir", "c:\\winutils\\")

    val conf = new SparkConf().setAppName("SparkSQL").setMaster("local").set("com.spark.executor", "")

    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)

    // loading the tweetfile

    val jsonFile = sqlContext.jsonFile("CollectedTweets.json")

    jsonFile.registerTempTable("TweetTable")






    //val hr=sqlContext.sql("select user.name,user.followers_count from TweetTable where text LIKE '%bitcoin%'")
    //val hrcount=hr.count()
    //println("bitcoin %s".format(hrcount))
    //hr.map(x=> (x(0),x(1))).coalesce(1,true).saveAsTextFile("src/main/resources/bitcoinintext")

    //val sep=sqlContext.sql("select user.name, SUBSTRING(user.created_at,5,3) AS month,SUBSTRING(user.created_at,12,8) AS time," +
      //"Count(*) as cnt from TweetTable where SUBSTRING(user.created_at,5,3) like '%Dec%' group by user")
    //sep.show()

    //val place =sqlContext.sql("select user.location, max(user.followers_count) AS followers_count from TweetTable where user.followers_count>100 group by user.location")
    //place.show()
    //place.map(x=> (x(0),x(1))).coalesce(1,true).saveAsTextFile("src/main/resources/locationusers")

    val post=sqlContext.sql("select user.name, SUBSTRING(user.created_at,5,3) AS month,SUBSTRING(user.created_at,12,8) AS time from TweetTable where text LIKE '%en%' group by user")
    post.show()
    post.map(x=> (x(0),x(1))).coalesce(1,true).saveAsTextFile("src/main/resources/month")

    val abc=sqlContext.sql("SELECT user.name AS Name,user.followers_count AS Followers_Count FROM TweetTable WHERE user.verified=true ORDER BY user.followers_count DESC LIMIT 10")
    abc.show()
    abc.map(x=> (x(0),x(1))).coalesce(1,true).saveAsTextFile("src/main/resources/Verified")

    val query6=
      sqlContext.sql("select user.favourites_count,place.country from TweetTable where user.location='United States'and place.country is not null ")
    query6.map(x=> (x(0),x(1))).coalesce(1,true).saveAsTextFile("src/main/resources/US")



    val query9 = sqlContext.sql("SELECT user.name,retweeted_status.truncated,retweet_count FROM TweetTable ")
    query9.show()
    query9.map(x=> (x(0),x(1))).coalesce(1,true).saveAsTextFile("src/main/resources/retweet1")

    val query2 = sqlContext.sql("SELECT user.name AS Name, user.id FROM TweetTable WHERE user.id BETWEEN 228582603 AND 333333333")
    query2.show()
    query2.map(x=> (x(0),x(1))).coalesce(1,true).saveAsTextFile("src/main/resources/id")




  }

}



