import org.apache.{spark => spark}
import myMacros.defineFields
object Main {
  def main(args: Array[String]): Unit = {
    @defineFields
    object AirportField
    val index = AirportField.index
    println(s"ahahahaha $index")
  }
}

