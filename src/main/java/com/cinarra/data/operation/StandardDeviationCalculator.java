package com.cinarra.data.operation;

import java.util.Iterator;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Buffer;
import cascading.operation.BufferCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

public class StandardDeviationCalculator extends BaseOperation implements Buffer
{

public StandardDeviationCalculator()
  {
  super( 1, new Fields( "event","stdDev" ) );
  }

public StandardDeviationCalculator( Fields fieldDeclaration )
  {
  super( 1, fieldDeclaration );
  }

public void operate( FlowProcess flowProcess, BufferCall bufferCall )
  {
  // init vars needed to calculate SD for an event
  double count = 0;
  
  int event=0;
  int value;
  double currentNum = 0;
  double numtotal = 0;
  double mean = 0;
  double square = 0, squaretotal = 0, sd = 0;
  
  // get all the current argument values for this grouping
  Iterator<TupleEntry> arguments = bufferCall.getArgumentsIterator();
  
  while( arguments.hasNext() )
    {
	  TupleEntry entry = arguments.next();
	  event = (int)Integer.parseInt(entry.getString("event"));
	  value = (int) Integer.parseInt(entry.getString("value"));
	  
	  // standard deviation calculation
	  
	  currentNum = value;
      numtotal = numtotal + currentNum;

      count++;
      mean = (double) numtotal / count;
      square = Math.pow(currentNum - mean, 2.0);
      squaretotal = squaretotal + square; 
      sd = Math.pow(squaretotal/count, 1/2.0);
    }

  // create a Tuple to hold our result values
  Tuple result = new Tuple(event,sd);
  
  // return the result Tuple
  bufferCall.getOutputCollector().add( result );
  }
}
