package simpleanonymizer

// row representation that keeps both raw objects and string values
case class RawRow(objects: Map[String, AnyRef], strings: Map[String, String])
