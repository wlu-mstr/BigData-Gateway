import org.apache.phoenix.schema.types.PDataType

object DataTypeTest extends App {
  val r: String = "r"
  println(PDataType.fromLiteral ("r"))
}