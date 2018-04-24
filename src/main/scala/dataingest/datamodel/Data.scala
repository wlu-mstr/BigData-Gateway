package dataingest.datamodel

/**
  * Created by dingbingbing on 4/23/18.
  */
case class Data(dataKey: DataKey,
                jsonRaw: String) {


}

object Data {
  def getKey(data: Data) = data.dataKey
}


