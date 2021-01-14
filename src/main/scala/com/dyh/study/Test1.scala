package com.dyh.study

class Test1 {
  def apply() = println("I is apply from class")
}

object Test1 {
  def apply() = println("I is apply from object")

  def main(args: Array[String]): Unit = {
    val testClass = new Test1()
//    testClass()
    val testObject = Test1()
//    testObject
  }
}
