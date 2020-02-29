package co.pacifik.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

object Final {
  
  def parseLine(line:String)= {
    val fields = line.split(";")
    
    val fechaHurto = fields(0)
    val municipio = fields(2)
    val armaEmpleada = fields(8)
    val movilAgresor = fields(9)
    val movilVictima = fields(10)
    val marcaMoto = fields(20)
    (fechaHurto, municipio, armaEmpleada, movilAgresor, movilVictima, marcaMoto)
  }
  
    /** Our main function where the action happens */
  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "Final")
    
    // Read each line of input data
    val lines = sc.textFile("/Users/nelsondiaz/scala/hurtos_motos_2017_2020.csv")
    
    val parsedLines = lines.map(parseLine)
    
    val municipios = parsedLines.map(x => (x._2))
        
    val loquesea = municipios.countByValue()
    // Reduce by stationID retaining the minimum temperature found
    //val minTempsByStation = municipios.reduceByKey( (x,y) => )
    
    // Collect, format, and print the results
    //val results = loquesea.collect()
    
    //results.sorted.foreach(println)
  }
}