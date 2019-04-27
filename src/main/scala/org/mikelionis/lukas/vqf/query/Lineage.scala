package org.mikelionis.lukas.vqf
package query

case class Lineage(decedent: Table, ancestors: Set[Table])
