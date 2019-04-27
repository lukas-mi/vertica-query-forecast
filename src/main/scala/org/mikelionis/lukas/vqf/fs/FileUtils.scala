package org.mikelionis.lukas.vqf
package fs

import java.io.File
import java.nio.file.Files

import spray.json._

object FileUtils {
  def readJSON[T: JsonReader](file: File): T = {
    val content = new String(Files.readAllBytes(file.toPath), "UTF-8")
    content.parseJson.convertTo[T]
  }
  def writeJSON[T: JsonWriter](file: File, obj: T): Unit = {
    val content = obj.toJson.prettyPrint.getBytes("UTF-8")
    Files.write(file.toPath, content)
  }
}
