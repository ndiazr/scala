package co.pacifik.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

object Final {
  
  def parseLine(line:String)= {
    val fields = line.split(";")
    
    val fechaHurto = fields(0).substring(6)
    val municipio = fields(2)
    val armaEmpleada = fields(8)
    val movilAgresor = fields(9)
    val zona = fields(6)
    val marcaMoto = fields(20)
    (fechaHurto, municipio, armaEmpleada, movilAgresor, zona, marcaMoto)
  }
  
    /** Our main function where the action happens */
  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "Final")
    
    // Read each line of input data
    val lines = sc.textFile("hurtos_motos_2017_2019.csv")
    
    val parsedLines = lines.map(parseLine)
    
    // robos realizados por año
    val anios = parsedLines.map(x => (x._1))
    val conteoAnios = anios.countByValue()
    val aniosOrdenados = conteoAnios.toSeq.sortBy(_._2)(Ordering.Long.reverse)
    
    // ciudades mas robos
    val municipios = parsedLines.map(x => (x._2))
    val conteoMunicipios = municipios.countByValue()
    val municipiosOrdenados = conteoMunicipios.toSeq.sortBy(_._2)(Ordering.Long.reverse)
    
    println("------------------------------------------------------------------------------------------------------")
    println("Las ciudades que presentaron más hurtos del 2017 al 2019 fueron:")
    for (x <- 0 to 4) {
      println(municipiosOrdenados(x)._1 + " -> " + municipiosOrdenados(x)._2)
    }
    
    // ciudades mas robos por años
    val municipioAnio = parsedLines.map(x => (x._1, x._2))
    val municipioAnioTmp = municipioAnio.mapValues(x => (x, 1))
    
    val anio2017 = municipioAnioTmp.filter(x => x._1 == "2017")
    val anio2017Tmp = anio2017.map(x => (x._2._1)).countByValue().toSeq.sortBy(_._2)(Ordering.Long.reverse)
    
    val anio2018 = municipioAnioTmp.filter(x => x._1 == "2018")
    val anio2018Tmp = anio2018.map(x => (x._2._1)).countByValue().toSeq.sortBy(_._2)(Ordering.Long.reverse)
    
    val anio2019 = municipioAnioTmp.filter(x => x._1 == "2019")
    val anio2019Tmp = anio2019.map(x => (x._2._1)).countByValue().toSeq.sortBy(_._2)(Ordering.Long.reverse)
    
    println("------------------------------------------------------------------------------------------------------")
    println("Las ciudades que presentaron más hurtos por año fueron:")
    println("-----------------------2017-----------------------")
    for (x <- 0 to 4) {
      println(anio2017Tmp(x)._1 + " -> " + anio2017Tmp(x)._2)
    }
    println("-----------------------2018-----------------------")
    for (x <- 0 to 4) {
      println(anio2018Tmp(x)._1 + " -> " + anio2018Tmp(x)._2)
    }
    println("-----------------------2019-----------------------")
    for (x <- 0 to 4) {
      println(anio2019Tmp(x)._1 + " -> " + anio2019Tmp(x)._2)
    }
    
    // cifras de motos vendidas por año
    val cifraVendidas2017 = 499692;
    val cifraVendidas2018 = 553361;
    val cifraVendidas2019 = 612086;
    
    // porcentaje de hurtos por año
    println("------------------------------------------------------------------------------------------------------")
    aniosOrdenados.foreach(x => {
      if (x._1 == "2019") {
        println("La cantidad de motos vendidas en el año 2019 fue :" + cifraVendidas2019 +
            " Cantidad de hurtos: " + x._2 + " Para un porcentaje de : " + cifraVendidas2019/x._2 + "%") 
      } else if (x._1 == "2018") {
        println("La cantidad de motos vendidas en el año 2018 fue :" + cifraVendidas2018 +
            " Cantidad de hurtos: " + x._2 + " Para un porcentaje de : " + cifraVendidas2018/x._2 + "%") 
      } else {
        println("La cantidad de motos vendidas en el año 2017 fue :" + cifraVendidas2017 +
            " Cantidad de hurtos: " + x._2 + " Para un porcentaje de : " + cifraVendidas2017/x._2 + "%") 
      }
    })
    
    // armas empleadas
    val armas = parsedLines.map(x => (x._3))
    val conteoArmas = armas.countByValue()
    val armasOrdenadas = conteoArmas.toSeq.sortBy(_._2)(Ordering.Long.reverse)

    println("------------------------------------------------------------------------------------------------------")
    println("Las armas más empleadas en los hurtos del 2017 al 2019 fueron:")
    for (x <- 0 to 4) {
      println(armasOrdenadas(x)._1 + " -> " + armasOrdenadas(x)._2)
    }
    
    // zonas con mas hurtos
    val zonas = parsedLines.map(x => (x._5))
    val conteoZonas = zonas.countByValue()
    val zonasOrdenadas = conteoZonas.toSeq.sortBy(_._2)(Ordering.Long.reverse)

    println("------------------------------------------------------------------------------------------------------")
    println("Las zonas donde se presentaron mas hurtos del 2017 al 2019 fueron:")
    zonasOrdenadas.foreach(x => {
      println(x._1 + " -> " + x._2)
    })
    
    // zonas vs armas usadas hurtos
    val zonasHurto = parsedLines.map(x => (x._5, x._3))
    val conteoZonasHurto = zonasHurto.countByValue()
    val zonasHurtoOrdenadas = conteoZonasHurto.toSeq.sortBy(_._2)(Ordering.Long.reverse)

    println("------------------------------------------------------------------------------------------------------")
    println("Las 3 armas mas usadas por zona del 2017 al 2019 fueron:")
    println("------------------------------------------------------------------------------------------------------")
    println("Zona Urbana:")
    val urbana = zonasHurtoOrdenadas.filter(x => x._1._1 == "URBANA")
    for (x <- 0 to 2) {
      println(urbana(x)._1._2 + " -> " + urbana(x)._2)
    }
    println("------------------------------------------------------------------------------------------------------")
    println("Zona Rural:")
    val rural = zonasHurtoOrdenadas.filter(x => x._1._1 == "RURAL")
    for (x <- 0 to 2) {
      println(rural(x)._1._2 + " -> " + rural(x)._2)
    }
    println("------------------------------------------------------------------------------------------------------")
    println("Otras Zonas:")
    val otras = zonasHurtoOrdenadas.filter(x => x._1._1 == "OTRAS")
    for (x <- 0 to 2) {
      println(otras(x)._1._2 + " -> " + otras(x)._2)
    }
    
    // marcas con mas hurtos
    val marcas = parsedLines.map(x => (x._6))
    val conteoMarcas = marcas.countByValue()
    val marcasOrdenadas = conteoMarcas.toSeq.sortBy(_._2)(Ordering.Long.reverse)

    println("------------------------------------------------------------------------------------------------------")
    println("Las marcas que presentaron mas cantidad de robos del 2017 al 2019 fueron:")
    for (x <- 0 to 4) {
      println(marcasOrdenadas(x)._1 + " -> " + marcasOrdenadas(x)._2)
    }
  }
}