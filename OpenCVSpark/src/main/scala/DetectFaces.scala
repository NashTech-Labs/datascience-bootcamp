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


    val inputPaths="/home/jouko/dev/project/TrainingSprints/datascience-bootcamp/OpenCVSpark/src/main/resources/images.txt"
    val rdd=sc.textFile(inputPaths)
    val rddFaces=rdd.map( path => (path, run(path)) )

    rddFaces.collect.foreach(println)
  }

  def run(imagePath: String): Int = {
    LibraryLoader.load
    val modelFile="/home/jouko/dev/project/TrainingSprints/datascience-bootcamp/OpenCVSpark/src/main/resources/haarcascade_frontalface_alt.xml"
    val faceDetector = new CascadeClassifier(modelFile)

    val image = Highgui.imread(imagePath)

    val faceDetections = new MatOfRect()
    faceDetector.detectMultiScale(image, faceDetections)

    faceDetections.toArray.length
  }
}

