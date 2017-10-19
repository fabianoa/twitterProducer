package br.gov.dataprev.twitter.spark;


import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.SparkContext._
import org.apache.spark.streaming.twitter._
import org.apache.spark.SparkConf

import org.apache.spark.Logging

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import twitter4j.Status
import org.apache.spark.streaming.Time
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.Minutes
import org.apache.spark.sql.catalyst.expressions.Minute
import twitter4j.conf.ConfigurationBuilder
import twitter4j.auth.AuthorizationFactory
import org.apache.spark.streaming.api.java.JavaStreamingContext
import org.apache.spark.Logging
import org.apache.spark.Logging
import org.apache.spark.sql.DataFrame
import org.apache.spark.ml.feature.RegexTokenizer
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.StopWordsRemover
import br.gov.dataprev.twitter.utils.DicionarioData
import br.gov.dataprev.twitter.util.TextUtils



object TwitterHashTagJoinSentiments extends Logging {
  
 
  val BASE  = "dicionarios"
  
  case class Twitter(id:Long,createAt:String,nome:String,latitude:Double,longitude:Double,descricao:String)
 
  
 def main(args: Array[String]) {

   //	System.setProperty("hadoop.home.dir", "C:\\hadoop-common-2.2.0-bin-master");  
    
   val consumerKey = "H8uzrzZGI8lzl9qZp4nl8791v"
   val consumerSecret = "X6x9CCL4PM3DacQILyx3SQErYUcFbJGv3ghkGgU5cSXU0qYMGb"
   val accessToken = "412468360-dyLD4IE5zDKkynOA8gSaHDhC6PkpCp9TbAIq8gZD"
   val accessTokenSecret = "wdLst7o4eqGsukqOjyxcQcyIjRZEqc02P0TlcAODPr01j"
   val url = "https://stream.twitter.com/1.1/statuses/filter.json"
 

   	val conf = new ConfigurationBuilder()
    conf.setOAuthAccessToken(accessToken)
    conf.setOAuthAccessTokenSecret(accessTokenSecret)
    conf.setOAuthConsumerKey(consumerKey)
    conf.setOAuthConsumerSecret(consumerSecret)
    conf.setStreamBaseURL(url)
    conf.setSiteStreamBaseURL(url)
    conf.setHttpProxyHost("10.70.180.23").setHttpProxyPort(80)
    
    

 
    val auth = AuthorizationFactory.getInstance(conf.build())

    System.setProperty("http.proxyHost", "10.70.180.23")
    System.setProperty("http.proxyPort", "80")
    System.setProperty("https.proxyHost", "10.70.180.23")
    System.setProperty("https.proxyPort", "80")
 
//  
//    // Set the system properties so that Twitter4j library used by twitter stream
//    // can use them to generat OAuth credentials
    System.setProperty("twitter4j.oauth.consumerKey", "H8uzrzZGI8lzl9qZp4nl8791v")
    System.setProperty("twitter4j.oauth.consumerSecret", "X6x9CCL4PM3DacQILyx3SQErYUcFbJGv3ghkGgU5cSXU0qYMGb")
    System.setProperty("twitter4j.oauth.accessToken", "412468360-dyLD4IE5zDKkynOA8gSaHDhC6PkpCp9TbAIq8gZD")
    System.setProperty("twitter4j.oauth.accessTokenSecret", "wdLst7o4eqGsukqOjyxcQcyIjRZEqc02P0TlcAODPr01j")
//    System.setProperty("twitter4j.http.proxyHost", "10.70.180.23");
//    System.setProperty("twitter4j.http.proxyPort", "80");
//    System.setProperty("twitter4j.http.useSSL", "true");
      
    
     setStreamingLogLevels()

    
    val config = new SparkConf().setAppName("Twitter Sentiment Collector")//.setMaster("local[*]")
    System.setProperty("hadoop.home.dir", "E:\\hadoop-2.7.1");   
    val sc = new SparkContext(config)
    val sqlContext = new SQLContext(sc)
   // val ssc = new StreamingContext(sc, Minutes(1))
    val ssc = new JavaStreamingContext(sc,Minutes(1))
   
    val lista_palavras_monitoradas = sc.textFile("palavras_monitoradas/lista_palavras_monitoradas.txt").collect().mkString.split(",") 
   
    val stream = TwitterUtils.createStream(ssc, auth,lista_palavras_monitoradas)
    
    
    val lanFilter = stream.dstream.filter(status => status.getUser.getLang == "pt")
    
    lanFilter.foreachRDD { (rdd: RDD[(Status)], time: Time) => 
  
       import sqlContext.implicits._
        
       val myDataset=rdd.map {t => Twitter(t.getId,
            t.getCreatedAt.toLocaleString(),
            t.getUser.getScreenName,
            if (t.getGeoLocation == null) 0 else t.getGeoLocation.getLatitude,
            if (t.getGeoLocation == null) 0 else t.getGeoLocation.getLongitude ,
            TextUtils.removerAcentos(t.getText)) }.toDF()
          
       processarSentimento(myDataset,"descricao",sqlContext,"/data/dataprev/sentimento_twitter") 
                                                                                                    
   } 

    ssc.start()
    ssc.awaitTermination()
  }
  
  /** Set reasonable logging levels for streaming if the user has not configured log4j. */
  def setStreamingLogLevels() {
    val log4jInitialized = Logger.getRootLogger.getAllAppenders.hasMoreElements
    if (!log4jInitialized) {
      // We first log something to initialize Spark's default logging, then we override the
      // logging level.
      logInfo("Setting log level to [WARN] for streaming example." +
        " To override add a custom log4j.properties to the classpath.")
      Logger.getRootLogger.setLevel(Level.OFF)
    }
  }
  
  
  def obterDicionarios(sqlContext: SQLContext){
    
          	  
	      	val parquetFile = sqlContext.parquetFile(DicionarioData.filePath("vocabulario_expandido"))
					
	      	parquetFile.registerTempTable("dicionario_expandido")

					val parquetFileAnewbrT = sqlContext.parquetFile("anew-br")
					parquetFileAnewbrT.registerTempTable("dicionario_anew")
	      
	}      
  
  
  def processarSentimento(dataFrame: DataFrame, nomeColunaTexto: String, sqlContext: SQLContext, localGravacao: String ): DataFrame = {

			
        obterDicionarios(sqlContext)

								
				val regex = new RegexTokenizer().setInputCol(nomeColunaTexto).setOutputCol("words").setMinTokenLength(3).setPattern("[a-zA-Z']+")
        .setGaps(false) // alternatively .setPattern("\\w+").setGaps(false)
        val regexed = regex.transform(dataFrame)
       
        val ColunasDataFrame = dataFrame.columns.mkString(",")  
      
        val stopWords = sqlContext.sparkContext.textFile("stopwords-br.txt").cache().toArray()
       	val remover = new StopWordsRemover().setStopWords(stopWords).setInputCol("words").setOutputCol("filtered")
		    val stopwordRemoved = remover.transform(regexed).registerTempTable("clipping")
    
		    
        val pipeline = new Pipeline().setStages(Array( regex, remover))
        val model = pipeline.fit(dataFrame).transform(dataFrame)
                         
        val tokenizedClippingsExploded = sqlContext.sql("SELECT *,explode(filtered) as palavra  FROM clipping")
				tokenizedClippingsExploded.registerTempTable("clippings_explodidos")
        
        val resultset = sqlContext.sql("SELECT "++ColunasDataFrame++",clippings_explodidos.palavra as palavra,negativo,positivo,valencia_media,alerta_media   FROM clippings_explodidos LEFT OUTER JOIN dicionario_expandido ON  clippings_explodidos.palavra = dicionario_expandido.palavra LEFT OUTER JOIN dicionario_anew ON  clippings_explodidos.palavra = dicionario_anew.palavra")

        resultset.show(100)
        
				if(resultset.count()>0)
			     resultset.write.parquet(localGravacao+"/"+java.time.LocalDate.now+"/"+java.time.LocalTime.now.getHour+"/"+System.currentTimeMillis())
        				
				return resultset

					
	}
  
  
  
  
  
  
}  

