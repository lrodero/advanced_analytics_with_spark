package com.hablapps.aawspark.ch09

import java.text.SimpleDateFormat
import java.io.File
import scala.io.Source

import com.github.nscala_time.time.Imports._

object DataInput {

	def readInvestingDotComHistory(file: File): Array[(DateTime,Double)] = {
    println(s"Reading investing.com history for ${file.getPath}")
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
    println(s"Reading yahoo history for ${file.getPath}")
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
    println(s"Reading stocks in folder ${folder.getPath}")
    folder.listFiles().flatMap ( f => { 
      try {
        Some(readYahooHistory(f))
      } catch {
        case e : Exception => println(s"CAUGHT EXCEPTION WHEN PROCESSING STOCK FILE ${f.getPath}:\n$e");None
      }
    }).filter(_.size >= 260*5+10)
  }

  def trimToTimeInterval(history: Array[(DateTime, Double)], start: DateTime, end: DateTime): Array[(DateTime, Double)] = {
    var trimmed = history.dropWhile(_._1 < start).takeWhile(_._1 <= end)
    if(trimmed.head._1 != start)
      trimmed = Array((start, trimmed.head._2)) ++ trimmed
    if(trimmed.last._1 != end)
      trimmed = trimmed ++ Array((end, trimmed.last._2))
    trimmed
  }
  
  // Fillin empty time periods in a given time interval.
  // "To deal with missing values within a time series, we use a simple imputation strategy
  // that fills in an instrument’s price as its most recent closing price before that day."
  def fillInHistory(history: Array[(DateTime, Double)], start: DateTime, end: DateTime): Array[(DateTime, Double)] = {
    val filled = new scala.collection.mutable.ArrayBuffer[(DateTime, Double)]()
    var cur = history
    var curDate = start
    while (curDate < end) {
      if (cur.tail.nonEmpty && cur.tail.head._1 == curDate) {
        cur = cur.tail
      }
      filled += ((curDate, cur.head._2))
      curDate += 1.days
      // Skip weekends
      if (curDate.dayOfWeek().get > 5) curDate += 2.days
    }
    filled.toArray
  }
  
  def twoWeekReturns(history: Array[(DateTime, Double)]): Array[Double] = {
    history.sliding(10). // 10 days instead of 14, as we do not include weekends
      map(window => window.last._2 - window.head._2).toArray
  }
  
  def factorMatrix(histories: Seq[Array[Double]]): Array[Array[Double]] = {
    val mat = new Array[Array[Double]](histories.head.length)
    for (i <- 0 until histories.head.length) {
      mat(i) = histories.map(_(i)).toArray
    }
    mat
  }

  
  def main(args: Array[String]):Unit = {
    
    // // READING INPUT DATA \\ \\

    val stocksFolder = new File("ch09-data/stocks-few/")
    val rawStocks = readStocksInFolder(stocksFolder)
  
    val factorsFolder = new File("ch09-data/factors/")
    // Read crude oil and US treasury bonds price history
    val factors1: Seq[Array[(DateTime, Double)]] =
      Array("CrudeOilPrice.tsv", "USTreasuryBonds30Y.tsv").
        map(x => new File(factorsFolder, x)).
        map(readInvestingDotComHistory)
    // Read S&P and NASDAQ indexes history
    val factors2: Seq[Array[(DateTime, Double)]] =
      Array("SNP.csv", "NDX.csv").
        map(x => new File(factorsFolder, x)).
        map(readYahooHistory)
  
    val start = new DateTime(2010, 1, 1, 0, 0)
    val end = new DateTime(2015, 12, 31, 0, 0)
    
    val stocks:Seq[Array[(DateTime,Double)]] = rawStocks.map(trimToTimeInterval(_,start,end)).map(fillInHistory(_,start,end))
    val factors:Seq[Array[(DateTime,Double)]] = (factors1 ++ factors2).map(trimToTimeInterval(_,start,end)).map(fillInHistory(_,start,end))
    
    println(s"STOCKS  READ: ${stocks.size}")
    println(s"FACTORS READ: ${factors.size}")
    
    val verify = (stocks ++ factors).forall(_.size == stocks(0).size)
    
    println("\nFACTORS FIRST VALUES:")
    factors.foreach(ar => {println; ar.take(10).foreach(println)})
    println("\nSTOCKS FIRST VALUES:")
    stocks.foreach(ar => {println; ar.take(10).foreach(println)})
    
    println(s"\nExpected size of all data items read: ${stocks(0).size}")
    println(s"VERIFICATION: ${if(verify) "CORRECT" else "WRONG"} ")
    
    val stocksReturns:Seq[Array[Double]] = stocks.map(twoWeekReturns)
    val factorsReturns:Seq[Array[Double]] = factors.map(twoWeekReturns) 
    
    // 'transposing' factor matrix, getting input ready for
    // least squares regression impl in apache commons math
  
    val factorMat:Array[Array[Double]] = factorMatrix(factorsReturns)
    println(s"\nFIRST ROW IN FACTOR MATRIX: ${factorMat(0).toList}")

    // // CREATING MODEL \\ \\

    def featurize(factorReturns: Array[Double]): Array[Double] = {
      val squaredReturns = factorReturns.map(x => math.signum(x) * x * x)
      val squareRootedReturns = factorReturns.map(x => math.signum(x) * math.sqrt(math.abs(x)))
      squaredReturns ++ squareRootedReturns ++ factorReturns
    }
    
    // 'tracking' additional data
    val factorFeatures = factorMat.map(featurize)
    println(s"\nFIRST ROW IN FACTOR FEATURES MATRIX: ${factorFeatures(0).toList}")

    // We will use Ordinary Least Squares (OLS) to estimate the parameters of a multiple linear regression model
    import org.apache.commons.math3.stat.regression.OLSMultipleLinearRegression
    def linearModel(instrument: Array[Double], factorMatrix: Array[Array[Double]]): OLSMultipleLinearRegression = {
      val regression = new OLSMultipleLinearRegression()
      regression.newSampleData(instrument, factorMatrix)
      regression
    }
    
    // Finally, we get the models that map factor returns to instrument returns (one model per instrument)
    val models:Seq[OLSMultipleLinearRegression] = stocksReturns.map(linearModel(_, factorFeatures))
    val factorWeights: Array[Array[Double]] = models.map(_.estimateRegressionParameters()).toArray
    
    
    // // SAMPLING \\ \\
    
    Drawing.plotDistribution(factorsReturns(0)) // Returns for US Treasury Bonds factor
    Drawing.plotDistribution(factorsReturns(1)) // Returns for crude oil factor

    import org.apache.commons.math3.stat.correlation.PearsonsCorrelation
    val factorCor = new PearsonsCorrelation(factorMat).getCorrelationMatrix().getData()
    println(s"\nPEARSONS CORRELATION:\n${factorCor.map(_.mkString("\t")).mkString("\n")}\n") // Pearson's shows that factors are _not_ independent
    
    import org.apache.commons.math3.stat.correlation.Covariance
    val factorCovariances = new Covariance(factorMat).getCovarianceMatrix().getData() // Factors covariances
    val factorMeans = factorsReturns.map(factor => factor.sum / factor.size).toArray  // Factors means

    import org.apache.commons.math3.distribution.MultivariateNormalDistribution
    val factorsDist:MultivariateNormalDistribution = new MultivariateNormalDistribution(factorMeans,factorCovariances) // Factors distribution for sampling

    println("A couple of samples of the factors distribution (a Multivariate Normal distribution)")
    println(s"${factorsDist.sample().mkString("[",",","]")}\n${factorsDist.sample().mkString("[",",","]")}")
    
    
    // // Running trials \\ \\
    
    val parallelism = 5
    val baseSeed = 1496
    val seeds:Range = (baseSeed until baseSeed + parallelism)
    
    import org.apache.spark.SparkContext
    import org.apache.spark.SparkContext._
    import org.apache.spark.SparkConf
    import org.apache.spark.rdd.RDD
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val seedRdd = sc.parallelize(seeds, parallelism)

    def trialReturns(seed: Long, numTrials: Int, instruments: Seq[Array[Double]], factorMeans: Array[Double], factorCovariances: Array[Array[Double]]): Seq[Double] = {
      
      def trialReturn(trialFeatures: Array[Double], instruments: Seq[Array[Double]]): Double = {
        
          def instrumentTrialReturn(instrument: Array[Double], trialFeatures: Array[Double]): Double = {
            var instrumentTrialReturn = instrument(0)
            var i = 0
            while (i < trialFeatures.length) {
              instrumentTrialReturn += trialFeatures(i) * instrument(i+1)
              i += 1
            }
            instrumentTrialReturn
          }
        
        var totalReturn = 0.0
        for (instrument <- instruments) {
          totalReturn += instrumentTrialReturn(instrument, trialFeatures)
        }
        totalReturn
      }
      
      import org.apache.commons.math3.random.MersenneTwister
      val rand = new MersenneTwister(seed)
      val multivariateNormal = new MultivariateNormalDistribution(rand, factorMeans, factorCovariances)
      val trialReturns = new Array[Double](numTrials)
      for (i <- 0 until numTrials) {
        val sample = multivariateNormal.sample()
        val trialFeatures = featurize(sample)
        trialReturns(i) = trialReturn(trialFeatures, instruments)
      }
      trialReturns
    }

    val numTrials = 25
    val bFactorWeights = sc.broadcast(factorWeights)
    val trials:RDD[Double] = seedRdd.flatMap(seed => trialReturns(seed, numTrials / parallelism, bFactorWeights.value, factorMeans, factorCovariances))
    println(s"\n\n\nRESULT OF $numTrials TRIALS: ${trials.collect().mkString("[",",","]")}")
    
    def fivePercentVaR(trials: RDD[Double]): Double = {
      val topLosses = trials.takeOrdered(math.max(trials.count().toInt / 20, 1))
      topLosses.last
    }
    val valueAtRisk = fivePercentVaR(trials)
    println(s"\nVALUE AT RISK: $valueAtRisk")
    
    def fivePercentCVaR(trials: RDD[Double]): Double = {
      val topLosses = trials.takeOrdered(math.max(trials.count().toInt / 20, 1))
      topLosses.sum / topLosses.length
    }
    val conditionalValueAtRisk = fivePercentVaR(trials)
    println(s"\nCONDITIONAL VALUE AT RISK: $conditionalValueAtRisk")

    Drawing.plotDistribution(trials)
    
  }
  
}
