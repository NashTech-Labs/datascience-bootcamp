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
  lazy val load = System.loadLibrary(Core.NATIVE_LIBRARY_NAME)
  //lazy val load = System.load(SparkFiles.get(Core.NATIVE_LIBRARY_NAME))
  //lazy val load = System.load(SparkFiles.get("opencv_java2413"))
  //lazy val load = System.load(SparkFiles.get("libopencv_java2413.so"))
}

object HelloOpenCV {
  def main(args: Array[String]): Unit = {
    //val conf = new SparkConf().setMaster("spark://192.168.1.5:7077").setAppName("OpenCVSpark")
    val conf = new SparkConf().setMaster("local[2]").setAppName("OpenCVSpark")
    val sc = new SparkContext(conf)

    val spark = SparkSession
        .builder()
        .appName("OpenCVSpark")
        .master("local[2")
        .getOrCreate()

    spark.sparkContext.setLogLevel("DEBUG")

    //LibraryLoader.load
    println("Hello, OpenCV")
    println(System.getProperty("java.library.path"))
    //System.loadLibrary(Core.NATIVE_LIBRARY_NAME)

    val imagePath1="/home/jouko/dev/project/TrainingSprints/datascience-bootcamp/OpenCVSpark/src/main/resources/skyfall.jpg"
    val imagePath2="/home/jouko/dev/project/TrainingSprints/datascience-bootcamp/OpenCVSpark/src/main/resources/lena.png"
    val imagePath3="/home/jouko/dev/project/TrainingSprints/datascience-bootcamp/OpenCVSpark/src/main/resources/BillGatesWarrenBufett.jpg"

    val paths=List(imagePath1, imagePath2, imagePath3)

    val rdd=sc.parallelize(paths)
    val rddFaces=rdd.map( path => (path, run(path)) )

    rddFaces.collect.foreach(println)
    //val numFaces=run(imagePath)
    //println("numFaces= " + numFaces)
  }

  def run(imagePath: String): Int = {
    LibraryLoader.load
    //val faceDetector = new CascadeClassifier(getClass.getResource("/lbpcascade_frontalface.xml").getPath)
    //val faceDetector = new CascadeClassifier(getClass."/home/jouko/dev/project/TrainingSprints/datascience-bootcamp/OpenCVSpark/src/main/resources/lbpcascade_frontalface.xml")
    val faceDetector = new CascadeClassifier("/home/jouko/dev/project/TrainingSprints/datascience-bootcamp/OpenCVSpark/src/main/resources/lbpcascade_frontalface.xml")

    val image = Highgui.imread(imagePath)

    println("width= " + image.width)

    val faceDetections = new MatOfRect()
    faceDetector.detectMultiScale(image, faceDetections)

    //println("Number of faces= " + faceDetections.toArray().size)
    //println(String.format("Detected %s faces", faceDetections.toArray().length))

    for (rect <- faceDetections.toArray()) {
      println("Found face")
      Core.rectangle(image, new Point(rect.x, rect.y), new Point(rect.x + rect.width, rect.y + rect.height), new Scalar(0, 255, 0))
    }

    // Save the visualized detection.
    val filename = "/home/jouko/dev/project/TrainingSprints/datascience-bootcamp/OpenCVSpark/faceDetectionSpark.jpg"
    System.out.println(String.format("Writing %s", filename))
    Highgui.imwrite(filename, image)

    println("nfaces= " +faceDetections.toArray.length)
    faceDetections.toArray.length
  }
}

