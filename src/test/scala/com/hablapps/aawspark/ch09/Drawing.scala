package com.hablapps.aawspark.ch09

object Drawing {
  import com.cloudera.datascience.risk.KernelDensity
  import breeze.plot._
  def plotDistribution(samples: Array[Double]) {
    val min = samples.min
    val max = samples.max
    val domain = Range.Double(min, max, (max - min) / 100).
    toList.toArray
    val densities = KernelDensity.estimate(samples, domain)
    val f = Figure()
    val p = f.subplot(0)
    p += plot(domain, densities)
    p.xlabel = "Two Week Return ($)"
    p.ylabel = "Density"
  }
  
  import org.apache.spark.rdd.RDD
  def plotDistribution(samples: RDD[Double]) {
    val stats = samples.stats()
    val min = stats.min
    val max = stats.max
    val domain = Range.Double(min, max, (max - min) / 100)
    .toList.toArray
    val densities = KernelDensity.estimate(samples, domain)
    val f = Figure()
    val p = f.subplot(0)
    p += plot(domain, densities)
    p.xlabel = "Two Week Return ($)"
    p.ylabel = "Density"
  }

}