package org.mikelionis.lukas.vqf
package query

import java.io.File

case class Query(file: File, analysed: AnalysedQuery, metadata: QueryMetadata)
