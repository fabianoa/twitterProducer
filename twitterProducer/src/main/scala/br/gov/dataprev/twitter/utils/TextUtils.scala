package br.gov.dataprev.twitter.util

import java.text.Normalizer

object  TextUtils {
  
 def removerAcentos(s: String):  String = {

 def semhtmltags = s.replaceAll("&Aacute","�")
.replaceAll("&aacute;","�")
.replaceAll("&Acirc;","�")
.replaceAll("&acirc;","�")
.replaceAll("&Agrave;","�")
.replaceAll("&agrave;","�")
.replaceAll("&Aring;","�")
.replaceAll("&aring;","�")
.replaceAll("&Atilde;","�")
.replaceAll("&atilde;","�")
.replaceAll("&Auml;","�")
.replaceAll("&auml;","�")
.replaceAll("&AElig;","�")
.replaceAll("&aeli;","�")
.replaceAll("&Eacute;","�")
.replaceAll("&eacute;","�")
.replaceAll("&Ecirc;","�")
.replaceAll("&ecirc;","�")
.replaceAll("&Egrave;","�")
.replaceAll("&egrave;","�")
.replaceAll("&Euml;","�")
.replaceAll("&euml;","�")
.replaceAll("&ETH;","�")
.replaceAll("&eth;","�")
.replaceAll("&Iacute;","�")
.replaceAll("&iacute;","�")
.replaceAll("&Icirc;","�")
.replaceAll("&icirc;","�")
.replaceAll("&Igrave;","�")
.replaceAll("&igrave;","�")
.replaceAll("&Iuml;","�")
.replaceAll("&iuml;","�")
.replaceAll("&Oacute;","�")
.replaceAll("&oacute;"," �")
.replaceAll("&Ocirc;","�")
.replaceAll("&ocirc�;","�")
.replaceAll("&Ograve;"," �")
.replaceAll("&ograve;","�")
.replaceAll("&Oslash;","�")
.replaceAll("&oslash;","�")
.replaceAll("&Otilde;","�")
.replaceAll("&otilde;","�")
.replaceAll("&Ouml;","o")
.replaceAll("&ouml;","�")
.replaceAll("&Uacute;"," �")
.replaceAll("&uacute;","�")
.replaceAll("&Ucirc;","�")
.replaceAll("&ucirc;","�")
.replaceAll("&Ugrave;","  �")
.replaceAll("&ugrave;"," �")
.replaceAll("&Uuml;","�")
.replaceAll("&uuml;","�")
.replaceAll("&Ccedil;","�")
.replaceAll("&ccedil;","�")
.replaceAll("&Ntilde;","�")
.replaceAll("&ntilde;","�")
.replaceAll("&lt;","<")
.replaceAll("&gt;",">")
.replaceAll("&amp;","&")
.replaceAll("&quot;","")
.replaceAll("&reg;","�")
.replaceAll("&copy;","�")
.replaceAll("&Yacute;","�")
.replaceAll("&yacute;","�")
.replaceAll("&THORN;","�")
.replaceAll("&thorn;","�")
.replaceAll("&szlig;","�")
.replaceAll("&nbsp;"," ")
.replaceAll("[\\p{C}]","")
.replaceAll("<a [^>]+>[^<]*<\\/a>","")
.replaceAll("<[^>]*>","")


return Normalizer.normalize(semhtmltags, Normalizer.Form.NFD).toLowerCase().replaceAll("[^\\p{ASCII}]", "")


 
 } 
 

 
  
}
