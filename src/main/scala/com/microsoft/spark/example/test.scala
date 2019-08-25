package com.microsoft.spark.example

import java.util.Stack

object test {
  def main(args: Array[String]): Unit = {
    val iv: Stack[String] = new Stack[String]()
    val jv: Stack[Integer] = new Stack[Integer]()
var j = 0
    val a = "111ab333cdefg"
   // for (i <- j to a.length  ){
    var i=0
    while (i < a.length){
      while(j < a.length && a(j)>='0' && a(j)<='9'){
       // print("j->",j)
        jv.push(java.lang.Integer.parseInt(a(j).toString))
        j+=1
        i=j
      }

     // println("i->",i)
      iv.push(a(i).toString)
      i+=1
      j=i
    }
println(iv)
    println(jv)

}}

