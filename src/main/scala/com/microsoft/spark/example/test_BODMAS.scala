package com.microsoft.spark.example

import java.util.Stack
import scala.collection.JavaConversions._

object test_BODMAS {
  def main(args: Array[String]): Unit = {

    // Function to check precedences :

    def hasPrecedence(op1: Char, op2: Char): Boolean = {
      if (op2 == '(' || op2 == ')') false
      else if ((op1 == '*' || op1 == '/') && (op2 == '+' || op2 == '-')) false
      else true
    }


    // Function to exectue all mathemetica lcalculation
    def applyOp(op: Char, b: Int, a: Int): Int = {
      op match {
        case '+' => a + b
        case '-' => a - b
        case '*' => a * b
        case '/' =>
          if (b == 0)
            throw new UnsupportedOperationException("Cannot divide by zero")
          a / b
      }
    }

    //Main method :

    def evaluate(expression: String): Int = {
      val tokens: Array[Char] = expression.replaceAll("\\s","").toCharArray()
      val values: Stack[Integer] = new Stack[Integer]()
      val ops: Stack[Character] = new Stack[Character]()
      var j=0
      var  i=0
     //for (i <- j until tokens.length) {
     while (i <  tokens.length) {
       //if (tokens(i) != ' ') "" //continue
        if (tokens(j) >= '0' && tokens(j) <= '9') {
          val sbuf: StringBuffer = new StringBuffer()
         while (i < tokens.length && tokens(j) >= '0' && tokens(j) <= '9'){
           sbuf.append(tokens(j))
           j+=1
           i=j

         }
          //sbuf.append(tokens(i))
          values.push(java.lang.Integer.parseInt(sbuf.toString))

        }
        else if (tokens(i) == '(') {
                ops.push(tokens(i))
                i+=1
                j=i
           }
        else if (tokens(i) == ')') {
          while (ops.peek() != '(') values.push(applyOp(ops.pop(), values.pop(), values.pop()))
          ops.pop()
          i+=1
          j=i
           }
        else if (tokens(i) == '+' || tokens(i) == '-' || tokens(i) == '*' || tokens(i) == '/') {
          // to top two elements in values stack
          while (!ops.empty() && hasPrecedence(tokens(i), ops.peek()))
            values.push(applyOp(ops.pop(), values.pop(), values.pop()))
          // Push current token to 'ops'.
          ops.push(tokens(i))
          i+=1
          j=i
        }
      }
      while (!ops.empty()) values.push(
        applyOp(ops.pop(), values.pop(), values.pop()))
      values.pop()
    }

    println(evaluate("3111 + 6"))
     println(evaluate("3132 + 6000 - 1345"))
      println(evaluate("30 * (81 - 51) + 12 - 18"))
  }
}



