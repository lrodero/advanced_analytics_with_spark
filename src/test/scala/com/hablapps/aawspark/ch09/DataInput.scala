package com.hablapps.aawspark.ch09

import java.text.SimpleDateFormat
import java.io.File
import scala.io.Source

import com.github.nscala_time.time.Imports._

object DataInput {

	def readInvestingDotComHistory(file: File): Array[(DateTime,Double)] = {
		val format = new SimpleDateFormat("MMM d, yyyy")
	  val lines = Source.fromFile(file).getLines().toSeq
		lines.tail.map(line => {
		  val cols = line.split('\t')
		  val date = new DateTime(format.parse(cols(0)))
		  val value = cols(1).toDouble
		  (date, value)
		}).reverse.toArray
	}
	
  def readYahooHistory(file: File): Array[(DateTime, Double)] = {
    val format = new SimpleDateFormat("yyyy-MM-dd")
    val lines = Source.fromFile(file).getLines().toSeq
    lines.tail.map(line => {
      val cols = line.split(',')
      val date = new DateTime(format.parse(cols(0)))
      val value = cols(1).toDouble
      (date, value)
    }).reverse.toArray
  }
  
  def readStocksInFolder(folder: File): Seq[Array[(DateTime,Double)]] = {
    folder.listFiles().flatMap ( f => { 
      try {
        Some(readYahooHistory(f))
      } catch {
        case e : Exception => None
      }
    }).filter(_.size >= 260*5+10)
  }
  
  val folder = new File("data/factors/")
  val factors1: Seq[Array[(DateTime, Double)]] =
    Array("CrudeOilPrice.tsv", "USTreasuryBonds30Y.tsv").
      map(x => new File(folder, x)).
      map(readInvestingDotComHistory)

  val factors2: Seq[Array[(DateTime, Double)]] =
    Array("SNP.csv", "NDX.csv").
      map(x => new File(folder, x)).
      map(readYahooHistory)

  
}