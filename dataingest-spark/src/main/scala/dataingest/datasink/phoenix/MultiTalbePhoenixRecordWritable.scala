package dataingest.datasink.phoenix

import java.sql.PreparedStatement

import org.apache.phoenix.schema.types._
import org.apache.phoenix.util.ColumnInfo
import org.joda.time.DateTime

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * Created by WLU on 4/24/2018.
  */
class MultiTalbePhoenixRecordWritable(val tableName: String, val columnMetaDataList: List[ColumnInfo]) {
  val upsertValues: ArrayBuffer[Any] = mutable.ArrayBuffer[Any]()

  def write(statement: PreparedStatement): Unit = {
    // Make sure we at least line up in size
    if (upsertValues.length != columnMetaDataList.length) {
      throw new UnsupportedOperationException(
        s"Upsert values ($upsertValues) do not match the specified columns (columnMetaDataList)"
      )
    }

    // Correlate each value (v) to a column type (c) and an index (i)
    upsertValues.zip(columnMetaDataList).zipWithIndex.foreach {
      case ((v, c), i) =>
        if (v != null) {

          // Both Java and Joda dates used to work in 4.2.3, but now they must be java.sql.Date
          // Can override any other types here as needed
          val (finalObj, finalType) = v match {
            case dt: DateTime => (new java.sql.Date(dt.getMillis), PDate.INSTANCE)
            case d: java.util.Date => (new java.sql.Date(d.getTime), PDate.INSTANCE)
            case _ => (v, c.getPDataType)
          }

          // Helper method to create an SQL array for a specific PDatatype, and set it on the statement
          def setArrayInStatement(obj: Array[AnyRef]): Unit = {
            // Create a java.sql.Array, need to lookup the base sql type name
            val sqlArray = statement.getConnection.createArrayOf(
              PDataType.arrayBaseType(finalType).getSqlTypeName,
              obj
            )
            statement.setArray(i + 1, sqlArray)
          }

          // Determine whether to save as an array or object
          (finalObj, finalType) match {
            case (obj: Array[AnyRef], _) => setArrayInStatement(obj)
            case (obj: mutable.ArrayBuffer[AnyVal], _) => setArrayInStatement(obj.map(_.asInstanceOf[AnyRef]).toArray)
            case (obj: mutable.ArrayBuffer[AnyRef], _) => setArrayInStatement(obj.toArray)
            case (obj: mutable.WrappedArray[AnyVal], _) => setArrayInStatement(obj.map(_.asInstanceOf[AnyRef]).toArray)
            case (obj: mutable.WrappedArray[AnyRef], _) => setArrayInStatement(obj.toArray)
            case (obj: Array[Int], _) => setArrayInStatement(obj.map(_.asInstanceOf[AnyRef]))
            case (obj: Array[Long], _) => setArrayInStatement(obj.map(_.asInstanceOf[AnyRef]))
            case (obj: Array[Char], _) => setArrayInStatement(obj.map(_.asInstanceOf[AnyRef]))
            case (obj: Array[Short], _) => setArrayInStatement(obj.map(_.asInstanceOf[AnyRef]))
            case (obj: Array[Float], _) => setArrayInStatement(obj.map(_.asInstanceOf[AnyRef]))
            case (obj: Array[Double], _) => setArrayInStatement(obj.map(_.asInstanceOf[AnyRef]))
            // PVarbinary and PBinary come in as Array[Byte] but they're SQL objects
            case (obj: Array[Byte], _: PVarbinary) => statement.setObject(i + 1, obj)
            case (obj: Array[Byte], _: PBinary) => statement.setObject(i + 1, obj)
            // Otherwise set as array type
            case (obj: Array[Byte], _) => setArrayInStatement(obj.map(_.asInstanceOf[AnyRef]))
            case _ => statement.setObject(i + 1, finalObj)
          }
        } else {
          statement.setNull(i + 1, c.getSqlType)
        }
    }
  }

  def getTableName: String = tableName

  def add(value: Any): Unit = {
    upsertValues.append(value)
  }

  // Empty constructor for MapReduce
  def this() = {
    this("", List[ColumnInfo]())
  }
}