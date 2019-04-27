package org.mikelionis.lukas.vqf
package logging

import org.log4s._

trait Logging {
  protected val log: Logger = getLogger(getClass)
}
