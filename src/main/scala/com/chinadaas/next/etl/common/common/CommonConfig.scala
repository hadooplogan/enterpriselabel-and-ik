package common

import java.util.ResourceBundle

object CommonConfig {

  final val BUNDLE_NAME: String = "common-config";
  final val RESOURCE_BUNDLE: ResourceBundle = ResourceBundle.getBundle(BUNDLE_NAME)

  def getValue(key: String): String = {
    RESOURCE_BUNDLE.getString(key).trim()
  }
}
