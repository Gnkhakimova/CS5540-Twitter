
import org.apache.spark.{SparkContext, SparkConf}

object WordCount {

  def main(args: Array[String]) {

    val sparkConf = new SparkConf().setAppName("WordCount").setMaster("local[*]")

    val sc=new SparkContext(sparkConf)

    val input=sc.textFile("/Volumes/Data/PrinciplesOfBigData/WordCountHashTags/hashtagsAndUrl.txt")

    val wc=input.flatMap(line=>{line.split(" ")}).map(word=>(word,1)).cache()

    val output=wc.reduceByKey(_+_)

    output.saveAsTextFile("sparkWordCountOutput")

    val o=output.collect()

    var s:String="Words:Count \n"
    o.foreach{case(word,count)=>{

      s+=word+" : "+count+"\n"

    }}

  }

}

