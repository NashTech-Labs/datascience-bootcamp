import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.opencv.core.Core
import org.opencv.core.Mat
import org.opencv.core.MatOfRect
import org.opencv.core.Point
import org.opencv.core.Rect
import org.opencv.core.Scalar
import org.opencv.highgui.Highgui
import org.opencv.objdetect.CascadeClassifier
import org.apache.spark.{SparkConf, SparkContext, SparkFiles}

object LibraryLoader {
  lazy val load = System.load(SparkFiles.get("libopencv_java2413.so"))
}

object CountFaces {
  def main(args: Array[String]): Unit = {

    val sparkMaster="spark://192.168.1.5:7077"
    //val sparkMaster="local[2]"

    val conf = new SparkConf().setMaster(sparkMaster).setAppName("OpenCVSpark")
    val sc = new SparkContext(conf)

    val spark = SparkSession
        .builder()
        .appName("OpenCVSpark")
        //.master("local[2")
        .master(sparkMaster)
        .getOrCreate()

    spark.sparkContext.setLogLevel("DEBUG")


    val homeDir=sys.env("HOME")
    val projectDir=homeDir + "/dev/project/TrainingSprints/datascience-bootcamp/OpenCVSpark"
    val resourcesDir=projectDir + "/src/main/resources"

    val modelFile=resourcesDir + "/haarcascade_frontalface_alt.xml"

    val inputPaths=resourcesDir + "/images_with_counts_2.txt"
    val rdd=sc.textFile(inputPaths).map( x => x.split(" ") )
    val rddFaces=rdd.map( path => (path(0), countFaces(path(0), modelFile), path(1).toInt) )
    val loss=calcError(rddFaces)

    rddFaces.collect.foreach(println)
    println("loss= " + loss)
  }

  def countFaces(imagePath: String, modelFile: String): Int = {
    LibraryLoader.load
    val faceDetector = new CascadeClassifier(modelFile)

    val image = Highgui.imread(imagePath)

    val faceDetections = new MatOfRect()
    faceDetector.detectMultiScale(image, faceDetections)

    faceDetections.toArray.length
  }

  def calcError(rddFaces: RDD[(String, Int, Int)]): Double = {
    val comparison=rddFaces.map( x => (x._2.toDouble, x._3.toDouble) )
    comparison.collect.foreach(println)
    val scores=comparison.map( x => math.abs(x._1-x._2)/x._1 )
    scores.sum/scores.count.toDouble
  }
}

