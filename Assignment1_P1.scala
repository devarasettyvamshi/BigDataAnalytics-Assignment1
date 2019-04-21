package BDA_Assignment1

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

object Assignment1_P1 {
  def main(args: Array[String]): Unit = {
    //Start the Spark context
    val conf = new SparkConf()
      .setAppName("WordCount")
      .setMaster("local")
    val sc = new SparkContext(conf)

    val TextFileRDD = sc.textFile("C:\\Users\\User\\IdeaProjects\\Spark\\src\\main\\scala\\BDA_Assignment1\\Text1.txt")

    val TextFileSplitWordsRDD = TextFileRDD.flatMap(line=>line.split(" "))

    val TextFileSplitWordsCountRDD = TextFileSplitWordsRDD.map(word=>(word,1)).reduceByKey(_+_)

    val TotalWordsInFile = TextFileSplitWordsRDD.count()

    val TextFileSplitWordsWithIndexRDD = TextFileSplitWordsRDD.zipWithIndex()

    val TextFileSplitWordsWithIndexAsKeyRDD = TextFileSplitWordsWithIndexRDD.map{case (k,v)=>(v,k)}

    val FirstTargetWordPartitionRDD = TextFileSplitWordsWithIndexAsKeyRDD.filter {
      _ match {
        case (k,v) => (k >= -9 && k < 0) || (k > 0 && k <= 9)
        case _ => false //if invalid input is provided
      }
    }

    val FirstTargetWordPartitionRemovingIndexWithCountWordRDD = FirstTargetWordPartitionRDD.map{case (k,v)=>v}.map(A=>(A,1)).reduceByKey(_+_)

    val TargetWord1 = TextFileSplitWordsWithIndexAsKeyRDD.lookup(0)

    val FirstTargetWordPartitionRemovingIndexWithCountWordAddingTargetWordRDD = FirstTargetWordPartitionRemovingIndexWithCountWordRDD.map{case (k,v)=>(v,k)}.mapValues(v=>(v,TargetWord1(0)))

    val TargetWordWordOccurenceRDD = FirstTargetWordPartitionRemovingIndexWithCountWordAddingTargetWordRDD.map(A=>(A._2._2,(A._2._1,A._1)))

    val JoinForFrequency = TargetWordWordOccurenceRDD.join(TextFileSplitWordsCountRDD)

    var TotalFrequncy = JoinForFrequency.map(A=>((A._2._1._1,A._1),(A._2._1._2).toFloat/A._2._2)).reduceByKey(_+_)

    for(i<-1 to TotalWordsInFile.toInt-1){
      val IthTargetWordPartitionRDD = TextFileSplitWordsWithIndexAsKeyRDD.filter {
        _ match {
          case (k,v) => (k >= i-9 && k < i) || (k > i && k <= i+9)
          case _ => false //incase of invalid input
        }
      }
      val IthTargetWordPartitionRemovingIndexWithCountWordRDD = IthTargetWordPartitionRDD.map{case (k,v)=>v}.map(A=>(A,1)).reduceByKey(_+_)

      val TargetWordIth = TextFileSplitWordsWithIndexAsKeyRDD.lookup(i)

      val IthTargetWordPartitionRemovingIndexWithCountWordAddingTargetWordRDD = IthTargetWordPartitionRemovingIndexWithCountWordRDD.map{case (k,v)=>(v,k)}.mapValues(v=>(v,TargetWordIth(0)))

      val TargetWordWordOccurenceRDD = IthTargetWordPartitionRemovingIndexWithCountWordAddingTargetWordRDD.map(A=>(A._2._2,(A._2._1,A._1)))

      val JoinForFrequency = TargetWordWordOccurenceRDD.join(TextFileSplitWordsCountRDD)

      val FrequencyIthTargetWordIth = JoinForFrequency.map(A=>((A._2._1._1,A._1),(A._2._1._2).toFloat/A._2._2)).reduceByKey(_+_)

      TotalFrequncy = TotalFrequncy.union(FrequencyIthTargetWordIth).reduceByKey(_+_)
    }

    for(j<-2 to 10){
      val TextFileRDD = sc.textFile("C:\\Users\\User\\IdeaProjects\\Spark\\src\\main\\scala\\BDA_Assignment1\\Text"+j+".txt")

      val TextFileSplitWordsRDD = TextFileRDD.flatMap(line=>line.split(" "))

      val TextFileSplitWordsCountRDD = TextFileSplitWordsRDD.map(word=>(word,1)).reduceByKey(_+_)

      val TotalWordsInFile = TextFileSplitWordsRDD.count()

      val TextFileSplitWordsWithIndexRDD = TextFileSplitWordsRDD.zipWithIndex()

      val TextFileSplitWordsWithIndexAsKeyRDD = TextFileSplitWordsWithIndexRDD.map{case (k,v)=>(v,k)}

      val FirstTargetWordPartitionRDD = TextFileSplitWordsWithIndexAsKeyRDD.filter {
        _ match {
          case (k,v) => (k >= j-9 && k < 0) || (k > 0 && k <= 9)
          case _ => false //if invalid input is provided
        }
      }

      val FirstTargetWordPartitionRemovingIndexWithCountWordRDD = FirstTargetWordPartitionRDD.map{case (k,v)=>v}.map(A=>(A,1)).reduceByKey(_+_)

      val TargetWord1 = TextFileSplitWordsWithIndexAsKeyRDD.lookup(0)

      val FirstTargetWordPartitionRemovingIndexWithCountWordAddingTargetWordRDD = FirstTargetWordPartitionRemovingIndexWithCountWordRDD.map{case (k,v)=>(v,k)}.mapValues(v=>(v,TargetWord1(0)))

      val TargetWordWordOccurenceRDD = FirstTargetWordPartitionRemovingIndexWithCountWordAddingTargetWordRDD.map(A=>(A._2._2,(A._2._1,A._1)))

      val JoinForFrequency = TargetWordWordOccurenceRDD.join(TextFileSplitWordsCountRDD)

      TotalFrequncy = TotalFrequncy.union(JoinForFrequency.map(A=>((A._2._1._1,A._1),(A._2._1._2).toFloat/A._2._2)).reduceByKey(_+_))
    }
    val TotalFrequencyInDescending = TotalFrequncy.map{case (k,v)=>(v,k)}.sortByKey(false)

    val Top100 = TotalFrequencyInDescending.take(100)

    val Top100WithWordTargetWordAsKey = Top100.map{case (k,v)=>(v,k)}

    sc.parallelize(Top100WithWordTargetWordAsKey).saveAsTextFile("C:\\Users\\User\\IdeaProjects\\Spark\\src\\main\\scala\\BDA_Assignment1\\output.txt")

    sc.stop()
  }
}
