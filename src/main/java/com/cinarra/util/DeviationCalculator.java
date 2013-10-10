package com.cinarra.util;

import java.util.Scanner;
/*
 * For testing purposes.
 */
public class DeviationCalculator {
	 public static void main(String[] args) {

	     Scanner in = new Scanner(System.in);
	     double currentNum = 0;
	     double numtotal = 0;
	     double count = 0;
	     double mean = 0;
	     double square = 0, squaretotal = 0, sd = 0;

	     System.out.println("Enter a series of double value numbers, ");
	     System.out.println("Enter anything other than a number to quit: ");

	     while (in.hasNextDouble()) 
	     {

	         currentNum = in.nextDouble();
	         numtotal = numtotal + currentNum;

	         count++;
	         mean = (double) numtotal / count;
	         square = Math.pow(currentNum - mean, 2.0);
	         squaretotal = squaretotal + square; 
	         sd = Math.pow(squaretotal/count, 1/2.0);
	     }

	     System.out.println("The mean is: " +mean);
	     System.out.println("The standard deviation is: " +sd);

	     }


}
