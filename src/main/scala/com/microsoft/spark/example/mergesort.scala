package com.microsoft.spark.example

object mergesort {
  def main(args: Array[String]): Unit = {
    val arr1 = Array(5,6,2,4,1,8,7,3,9)

    def merge(a:Array[Int],b:Array[Int],arr:Array[Int]):Array[Int] = {
    val nl =a.length
      val nr = b.length
  var i,j,k=0
      while(i< nl && j < nr ){
        if (a(i) <= b(j)){
          arr(k) = a(i)
          i=i+1
        }
        else {
          arr(k)= b(j)
          j=j+1
        }
        k=k+1
      }
      while(i< nl ) {
        arr(k) = a(i)
        i=i+1
        k=k+1
      }
      while(j<nr){
        arr(k) = b(j)
        k=k+1
      }
arr
    }

    def mergesort (arr:Array[Int]): Array[Int] ={
      var n = arr.length
      if(n<2) {
        arr
      }
      var mid = n/2
      var left = Array[Int](mid)
      var right = Array[Int]( n - mid)
      for (i<- 0 to mid -1){
        left(i) = arr(i)

      }
      for ( i<- mid to n-1){
        right(i-mid ) = arr(i)
      }

      mergesort(left)
      mergesort(right)
      merge(left,right,arr)
      arr
    }

    mergesort(arr1)
  }

}
