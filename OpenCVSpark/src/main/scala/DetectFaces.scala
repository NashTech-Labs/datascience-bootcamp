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
  lazy val load = System.load(SparkFiles.get(Core.NATIVE_LIBRARY_NAME))
}

object HelloOpenCV {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("OpenCVSpark")
    val sc = new SparkContext(conf)

    println("Hello, OpenCV")
    println(System.getProperty("java.library.path"))
    //System.loadLibrary(Core.NATIVE_LIBRARY_NAME)

    val imagePath1="/home/jouko/dev/project/TrainingSprints/datascience-bootcamp/OpenCVSpark/src/main/resources/skyfall.jpg"
    val imagePath2="/home/jouko/dev/project/TrainingSprints/datascience-bootcamp/OpenCVSpark/src/main/resources/lena.png"

    val paths=List(imagePath1, imagePath2)

    val rdd=sc.parallelize(paths)
    val rddFaces=rdd.map( path => (path, run(path)) )

    rddFaces.collect.foreach(println)
    //val numFaces=run(imagePath)
    //println("numFaces= " + numFaces)
  }

  def run(imagePath: String): Int = {
    LibraryLoader.load
    val faceDetector = new CascadeClassifier(getClass().getResource("/lbpcascade_frontalface.xml").getPath())

    val image = Highgui.imread(imagePath)

    val faceDetections = new MatOfRect()
    faceDetector.detectMultiScale(image, faceDetections)

    //println("Number of faces= " + faceDetections.toArray().size)
    //println(String.format("Detected %s faces", faceDetections.toArray().length))
    /*    
    for (rect <- faceDetections.toArray()) {
      Core.rectangle(image, new Point(rect.x, rect.y), new Point(rect.x + rect.width, rect.y + rect.height), new Scalar(0, 255, 0))
    }

    // Save the visualized detection.
    val filename = "faceDetection.png"
    System.out.println(String.format("Writing %s", filename))
    Highgui.imwrite(filename, image)
    */
    faceDetections.toArray.size
  }
}

