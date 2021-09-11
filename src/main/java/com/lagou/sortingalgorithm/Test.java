package com.lagou.sortingalgorithm;

public class Test {
    public static void main(String[] args) {
        Integer[] array = new Integer[10];
        for (int i= 10;i>0;i--){
            array[10 - i] = i;
        }
        System.out.println("输入 ");
        for (Integer integer : array) {
            System.out.print(integer + " ");
        }
        System.out.println("");
        System.out.println("输出 ");
        MergeSort<Integer> integerMergeSort = new MergeSort<>();
        integerMergeSort.sort(array);
    }
}
