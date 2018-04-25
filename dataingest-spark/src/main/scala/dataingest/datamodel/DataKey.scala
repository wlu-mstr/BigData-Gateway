package dataingest.datamodel

/**
  * Created by dingbingbing on 4/23/18.
  */
case class DataKey(appKey: String,
                   eventType: String) {

  def key = appKey + "|" + eventType
}
