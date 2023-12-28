# Module 2 lab

## Assignment 1: Single Stream, Operation on elements

The goal of this assignment is to process and convert historical 3-month T-Bill yields taken from the US treasury department with prices. This result will then be used as a basis for the subsequent assignments. 

Example of calcs - data/tbill91_daily.xlsx

### Task

* Read in the data as a stream, convert daily closing yield to price and output to the console. 
* Create a simple unit test to check the output. 

### Input Data

* For this assignment you will work with a single data stream.
* It is a CSV file of the daily high, low, open, closing yields for the 3-month T-bill from 2000 to present
* See SourceFunction [TBillRateSource](src/main/java/com/riskfocus/training/assignments/sources/TBillRateSource.java)

### Expected Output

The result of this exercise is a data stream of Tuple3<Date, Double, Double> or (Date, Yield, Price)

The resulting stream should be printed to standard out.

## Assumptions

* The data used for input is an example of bounded (finite) stream and would normally be processed as a DataSet.
* Use [endOfMonth](src/main/java/com/riskfocus/training/assignments/domain/TBillRate.java) a marker for the end of month to trigger computation
* T-bills prices are discounted from par (assumed to be $100) using a simple interest formula. 
* The price can be calculated as: P = 100 -  r * t where r is the yield in decimal and t is 91/360 years (3 months in the 30/360 day count convention)


- [Windows](https://ci.apache.org/projects/flink/flink-docs-stable/dev/stream/operators/windows.html)
- [See the section on aggregations on windows](https://ci.apache.org/projects/flink/flink-docs-stable/dev/stream/operators/#datastream-transformations)

## Flink Pipeline

* Start point [TBillPriceAssignment](src/main/java/com/riskfocus/training/assignments/tbillprices/TBillPriceAssignment.java)
-----
