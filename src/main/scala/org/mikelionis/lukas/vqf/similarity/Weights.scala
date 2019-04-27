package org.mikelionis.lukas.vqf
package similarity

case class Weights(
    projection: Double,
    selection: Double,
    groupBy: Double
)

object Weights {
  val default = Weights(0.3, 0.4, 0.3)

  def apply(projection: Double, selection: Double, groupBy: Double): Weights = {
    require(projection + selection + groupBy == 1.0, "Coefficient sum has to be equal to 1.0")
    new Weights(projection, selection, groupBy)
  }
}
