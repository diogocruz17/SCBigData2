import com.uber.h3core.H3Core
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import java.sql.Timestamp

object Lab1 {

    case class ElementRef(
                             ref: Long
                         )

    case class ElementMember(
                                `type`: String,
                                ref:    Long,
                                role:   String
                            )

    case class OrcSchemaClass(
                                 id:        Long,
                                 `type`:    String,
                                 tags:      Map[String, String],
                                 lat:       Double,
                                 lon:       Double,
                                 nds:       Array[ElementRef],
                                 members:   Array[ElementMember],
                                 changeSet: Long,
                                 timestamp: Timestamp,
                                 uid:       Long,
                                 user:      String,
                                 version:   Long,
                                 visible:   Boolean
                             )

    val orcSchema: StructType = new StructType()
        .add("id", LongType,                          nullable = false)
        .add("type", StringType,                      nullable = true)
        .add("tags", MapType(StringType, StringType), nullable = true) // TODO: Change StringType to TagType
        .add("lat", DoubleType,                       nullable = false)
        .add("lon", DoubleType,                       nullable = false)
        .add("nds", ArrayType(new StructType()
            .add("ref", LongType)
        )
        )
        .add("members", ArrayType(
            new StructType()
                .add("type", StringType)
                .add("ref",  LongType)
                .add("role", StringType)
        ), nullable = true
        )
        .add("changeSet", LongType,      nullable = true)
        .add("timestamp", TimestampType, nullable = true)
        .add("uid",       LongType,      nullable = true)
        .add("user",      StringType,    nullable = true)
        .add("version",   LongType,      nullable = true)
        .add("visible",   BooleanType,   nullable = true)

    case class ParquetSchemaClass(
                                     lat:       Double,
                                     lon:       Double,
                                     elevation: Int
                                 )

    val parquetSchema: StructType =
        StructType(
            Array(
                StructField("lat",       DoubleType,  nullable = false),
                StructField("lon",       DoubleType,  nullable = false),
                StructField("elevation", IntegerType, nullable = false)
            )
        )

    def main(args: Array[String]) {
        // Create a SparkSession
        val spark: SparkSession = SparkSession.builder.appName("Lab2").getOrCreate
        spark.sparkContext.setLogLevel("ERROR")
        spark.sql("set spark.sql.files.ignoreCorruptFiles=true")
        import spark.implicits._

        val SEALEVEL: Int = parseSeaLevel(args(0))
        println("Sealevel increase:" + SEALEVEL)
        val S3: String = "s3://abs-tudelft-sbd-2022"
        val MY_S3: String = "s3://sbd-group-42"

        //[place, num_evacuees, city_type, population, h3index, lat, lon, harbour]
        // Load orc file
        val dataSetCities = spark
            .read
            .schema(orcSchema)
            .orc(S3 + "/" + args(1) + ".orc")
            .as[OrcSchemaClass]

            // remove bad rows such as no latitude or longitude because those rows can not give us a h3index
            .filter(col("lat").isNotNull
                && col("lon").isNotNull)

            //We need harbours and cities, towns, villages and hamlets. Rest can go.
            .filter(col("tags.harbour") === "yes"
                || (col("tags.population").isNotNull
                    && ((col("tags.place") === "city")
                        || (col("tags.place") === "town")
                        || (col("tags.place") === "village")
                        || (col("tags.place") === "hamlet")
                        )
                    ))

            // Get h3index and only keep columns we need.
            .withColumn("h3index", getIndexUDF(col("lat"), col("lon"))) //Move this to the top?
            .select("tags.name", "tags.population", "h3index", "lat", "lon", "tags.place", "tags.harbour")

            // Rename columns to suitable names
            .withColumnRenamed("place", "city_type")
            .withColumnRenamed("name", "place")
            .withColumnRenamed("population", "num_evacuees")

            // Type cast columns so that we can filter them faster later, example we only want to know
            // if a place is harbour for which a primitive type like boolean is better than reference type string
            .withColumn("harbour", col("harbour").isNotNull)
            .withColumn("num_evacuees", col("num_evacuees").cast(IntegerType))
            .withColumn("city_type", col("city_type") === "city")

        // Load parquet file and get h3index from latitude and longitudes
        val dataSetElevation = spark.read
            .schema(parquetSchema)
            .parquet(S3 + "/ALPSMLC30.parquet/")
            .as[ParquetSchemaClass]
            .filter(col("lat") > args(2).toDouble
                && col("lat") < args(3).toDouble
                && col("lon") > args(4).toDouble
                && col("lon") < args(5).toDouble)
            .withColumn("h3index", getIndexUDF(col("lat"), col("lon")))

            // select columns we need and find average elevation of h3 index
            .select("h3index", "elevation")
            .groupBy("h3index")
            .mean("elevation")

            // rename columns to suitable names
            .withColumnRenamed("avg(elevation)", "elevation")
            .withColumnRenamed("h3index", "h3indexElevation")


        /////////////////////////////
        /// Adequate Requirements ///
        /////////////////////////////
        println("adequate requirements")

        // [place, num_evacuees, h3index, lat, lon, elevation]
        // DataSet to meet adequate requirements of assignment 1
        val dataSetAdequate = dataSetCities
            //.repartition(20)
            .join(dataSetElevation.select("h3indexElevation", "elevation"),
                (col("h3index") === col("h3indexElevation")),
                "left")

            // drop unrequired column, cache and force scala to compute
            .drop(col("h3indexElevation"))
            .cache()

        //dataSetAdequate.show(false)

        /////////////////////////
        /// Good Requirements ///
        /////////////////////////
        println("good requirements")

        // Find safe cities (above sealevel)
        val safeCities = dataSetAdequate
            .filter(col("city_type"))
            .filter(col("elevation") > SEALEVEL)

            // rename columns to prevent ambiguous reference when joining
            .withColumnRenamed("lat", "latitude")
            .withColumnRenamed("lon", "longitude")
            .withColumnRenamed("place", "destination")
            .withColumnRenamed("num_evacuees", "old_population")

            // select columns needed later, cache and force scala to compute
            .select("destination", "latitude", "longitude", "old_population")
            .cache()

        //safeCities.show(false)

        // find unsafe places (below sealevel)
        val unsafePlaces = dataSetAdequate
            .filter(!col("harbour")
                 &&  col("elevation") <= SEALEVEL)

            // select columns need later, cache and force scala to compute
            .select("place", "lat", "lon", "num_evacuees")
            .cache()

        // Calculate total number of evacuees
        println("-------------------------------------------------")
        println("Population that need to move: " + unsafePlaces.agg(sum("num_evacuees")).first.get(0))
        println("-------------------------------------------------")

        // find harbours
        val dataSetHarbour = dataSetAdequate
            .filter(col("harbour"))

            // rename columns to prevent ambiguous reference when joining
            .withColumnRenamed("lat", "latitude")
            .withColumnRenamed("lon", "longitude")

            // keep columns we need (we don't need name of harbour because it gets renamed to Waterworld)
            .select("latitude", "longitude")
            .cache()

        
        //dataSetHarbour.show(false)
        

        import org.apache.spark.sql.expressions.Window

        //[place, num_evacuees, lat, lon, destination, old_population, DistanceToCity]
        // join unsafePlaces with safeCities to find nearest safeCity
        val goodDataSet = unsafePlaces.as("unsafe")
            .join(safeCities.as("safe"))

            // select columns needed and find distances to safe city
            .select($"unsafe.place", $"unsafe.num_evacuees", $"safe.destination", $"unsafe.lat", $"unsafe.lon", $"safe.old_population",
                getDistanceUDF($"safe.latitude", $"safe.longitude", $"unsafe.lat", $"unsafe.lon") // TODO: can probably optimize this through col?
                .as("distance_to_city"))

            // find closest safe city
            .withColumn("min(distance_to_city)", min(col("distance_to_city")).over(Window.partitionBy($"place")))
            .where(col("distance_to_city") === col("min(distance_to_city)"))

            // Keep columns needed and cache
            .drop("min(distance_to_city")
            .select(col("place"), col("num_evacuees"), col("destination"), col("distance_to_city"), col("lat"), col("lon"), col("old_population"))
            .cache()


        //goodDataSet.show(40, false)



        //////////////////////////////
        /// Excellent requirements ///
        //////////////////////////////
        println("excellent requirements")

        // [place, num_evacuees, destination, go_to_harbour, old_population]
        // Last join to find harbours that are closer than some safeCities
        val dataSetFinalJoin = goodDataSet
            .as("unsafe")
            .join(dataSetHarbour.as("water"))

            // Select columns needed for join and use udf to find distances (needed to find closest harbour)
            .select($"unsafe.place", $"unsafe.num_evacuees", $"unsafe.distance_to_city", $"unsafe.destination", $"unsafe.old_population", $"unsafe.lat", $"unsafe.lon",
                getDistanceUDF($"water.latitude", $"water.longitude", $"unsafe.lat", $"unsafe.lon")
                .as("distance_to_harbour"))

            // Find unsafe places that have a harbour closer than closest city and drop unrequired column
            .withColumn("min(distance_to_harbour)", min(col("distance_to_harbour")).over(Window.partitionBy($"place")))
            .where(col("distance_to_harbour") === col("min(distance_to_harbour)"))
            .drop("min(distance_to_harbour")

            // Mark evacuees that should go to Waterworld, select columns needed and cache
            .withColumn("go_to_harbour", col("distance_to_harbour") < col("distance_to_city"))
            .select(col("place"), col("num_evacuees"), col("destination"), col("go_to_harbour"), col("old_population"))
            .cache()

        //dataSetFinalJoin.show(100,false)

        //[place, num_evacuees, destination, old_population]
        // Calculate evacuees going to Waterworld
        val waterWorldEvacuees = dataSetFinalJoin
            .filter(col("go_to_harbour"))
            .select("place", "num_evacuees")

            // Rename columns to appropriate names and calculate quarter of evacuees
            .withColumn("destination", lit("Waterworld"))
            .withColumn("old_population", lit(0))
            .withColumn("num_evacuees", (col("num_evacuees") * 0.25).cast(IntegerType))
            .cache()

        //[place, num_evacuees, destination, old_population]
        // Calculate remaining evacuees after quarter leave for Waterworld
        val dataSetExcellent = dataSetFinalJoin
            .withColumn("num_evacuees", when(col("go_to_harbour"), (col("num_evacuees") * 0.75).cast(IntegerType)))
            .select("place", "num_evacuees", "destination", "old_population")
            .cache()

        //waterWorldEvacuees.show()
    
        /////////////////////
        /// Write to file ///
        /////////////////////

        // Write first orc file
        dataSetExcellent
            .select("place", "num_evacuees", "destination")
            .write
            .mode("overwrite")
            .orc(MY_S3 + "/output/" + args(6) + "_excellent1.orc")
        

        waterWorldEvacuees
            .select("place", "num_evacuees", "destination")
            .write
            .mode("append")
            .orc(MY_S3 + "/output/" + args(6) + "_excellent1.orc")
        println("excellent1 written")

        // Calculate number of evacuees to destination
        val dataSet13 = dataSetExcellent
            .groupBy("destination", "old_population")
            .sum("num_evacuees")
            .withColumnRenamed("sum(num_evacuees)", "new_population")

            // safeCities to which no evacuee go as they are not needed, select required columns and cache
            .filter(col("new_population").isNotNull)
            .select("destination", "old_population", "new_population")

        val dataSet13_2 = waterWorldEvacuees
            .groupBy("destination", "old_population")
            .sum("num_evacuees")
            .withColumnRenamed("sum(num_evacuees)", "new_population")

            .select("destination", "old_population", "new_population")


        // write second orc file
        dataSet13
            .write
            .mode("overwrite")
            .orc(MY_S3 + "/output/" + args(6) + "_excellent2.orc")

        dataSet13_2
            .write
            .mode("append")
            .orc(MY_S3 + "/output/" + args(6) + "_excellent2.orc")
        println("excellent2 written")

//        spark.read.orc("./output/excellent1.orc").show(false)
//        spark.read.orc("./output/excellent2.orc").show(false)

  //    Stop the underlying SparkContext
        spark.stop
    }

    def parseSeaLevel(x: String): Int = {
        try {
            return x.toInt
        } catch {
            case e: Throwable => {
                println("Enter an integer: " + e)
                System.exit(1)
            }
        }
        42
    }
    object Holder {
        def h3(): H3Core = H3Core.newInstance()
    }

    val getIndexFromH3UDF: (Double, Double) => Long = (latitude: Double, longitude: Double) => Holder.h3().latLngToCell(latitude, longitude, 9)

    val getIndexUDF: UserDefinedFunction = udf(getIndexFromH3UDF)

    val getDistance: (Double, Double, Double, Double) => Double = (x1: Double, y1: Double, x2: Double, y2: Double) => Math.sqrt(Math.pow(Math.abs(x2 - x1),2) + Math.pow(Math.abs(y2 - y1),2))

    val getDistanceUDF: UserDefinedFunction = udf(getDistance)
}
