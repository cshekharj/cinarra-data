package com.cinarra.data;

import java.io.File;
import java.io.FileFilter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Properties;

import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextOutputFormat;

import cascading.flow.Flow;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.operation.Insert;
import cascading.operation.regex.RegexSplitter;
import cascading.operation.text.DateFormatter;
import cascading.operation.text.DateParser;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.AverageBy;
import cascading.property.AppProps;
import cascading.scheme.hadoop.TextLine;
import cascading.tap.MultiSourceTap;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tap.hadoop.Lfs;
import cascading.tuple.Fields;

import com.cinarra.data.operation.StandardDeviationCalculator;

public class WeeklyStatsGenerator {
	
	public static void main(String[] args) {
		// set the current job jar
		
		JobConf jobConf = new JobConf();
		jobConf.setOutputFormat(TextOutputFormat.class);
		
	    Properties properties = new Properties();
	    AppProps.setApplicationJarClass( properties, WeeklyStatsGenerator.class );
	    AppProps.appProps().buildProperties( jobConf );

	    String inputDir = "./src/main/resources";
	    String outputPath = "./output/weeklystats";
	    
	    //get all log files in log dir
	    File dir = new File(inputDir);
	    FileFilter fileFilter = new WildcardFileFilter("*.log");
	    File[] files = dir.listFiles(fileFilter);
	    
	    
	    // define what the input file looks like, "offset" is bytes from beginning
	    TextLine scheme = new TextLine( new Fields( "offset", "line" ) );
	    
	    //load all log files in source
	    List<Tap> sourceTaps = new ArrayList<Tap>()  ;
	    
	    for(File file:files){
	    	System.out.println("Found log file -"+file.getAbsolutePath());
	    	// create SOURCE tap to read a resource from the local file system, if input is not an URL
	    	Tap source = file.getAbsolutePath().matches( "^[^:]+://.*" ) ? new Hfs( scheme, file.getAbsolutePath() ) : new Lfs( scheme, file.getAbsolutePath() );
	    	sourceTaps.add(source);
	    }
	    
	    Tap [] sources = sourceTaps.toArray(new Tap[0]);
		Tap sourceTap = new MultiSourceTap(sources);
		
	    //tokenizer
	    RegexSplitter regexSplitter = new RegexSplitter( new Fields("datetime","event","value" ),";" );
	    
	    // date parser to remove time from datetime
	    DateParser dateParser = new DateParser( new Fields( "datetime" ),"dd.MM.yyyy"  );
	    
	    //formatter  to format date in output
	    DateFormatter formatter = new DateFormatter( new Fields( "datetime" ), "dd.MM.yyyy" );
	    
	    //split  loglines into tokens
		   
	    Pipe logline = new Each( "eventSum", new Fields( "line" ), regexSplitter,Fields.RESULTS );
	     
	   
	    // statistics - 1. Compute Average for event group for all files - i.e. is weekly as we process only this week's files
	    
		// for weekly just get the events and values irrespective of date
	    Fields groupingFields = new Fields( "event" );
	    Fields valueField = new Fields( "value" );
	    Fields avgField = new Fields( "weekly-event-avg" );
	    // add up all event values grouped by date and event id
	    Pipe avgAssembly = new AverageBy( logline, groupingFields, valueField, avgField);
	   
	    //get the start of the week - Monday for current date
	    DateFormat dateFormat = new SimpleDateFormat("dd.MM.yyyy");
	    Calendar cal = Calendar.getInstance();
	    
	    for(int i=0;cal.getTime().getDay() >Calendar.SUNDAY;i++){
			   cal.add(Calendar.DAY_OF_WEEK,-1);
		}
	    //create new date value for each output record
	    Insert weekStartDateInsert = new Insert( new Fields( "weekStartDate" ), dateFormat.format(cal.getTime()) );
	    //add week startdate to output
	    avgAssembly = new Each( avgAssembly, weekStartDateInsert, Fields.ALL );
	    // output path, replace if output is present
	    Tap tsSinkTap = new Hfs( new TextLine(), outputPath+"/avg-"+dateFormat.format(cal.getTime()),SinkMode.REPLACE );
	    
	    // statistics - 2. Compute standard deviation for events by values
	    
	    Fields deveGrpFields = new Fields( "event" );
	    Fields sortFields = new Fields( "value" );
	    Pipe stdDevAssembly = new GroupBy( logline, deveGrpFields,sortFields);
	    
	    StandardDeviationCalculator stdCalc = new StandardDeviationCalculator(new Fields( "event","stdDev" ));
	    stdDevAssembly = new Every(stdDevAssembly, stdCalc, Fields.RESULTS);
	    stdDevAssembly = new Each(stdDevAssembly,weekStartDateInsert,Fields.ALL);
	    
	    Tap tsDevSinkTap = new Hfs( new TextLine(), outputPath+"/std-dev-"+dateFormat.format(cal.getTime()),SinkMode.REPLACE );
	    
	    // connect the assembly to the source and sink taps
	    Flow weeklyStdDevFlow = new HadoopFlowConnector( properties ).connect( sourceTap, tsDevSinkTap, stdDevAssembly );
	   
	    Flow weeklyAvgerageFlow = new HadoopFlowConnector( properties ).connect( sourceTap, tsSinkTap, avgAssembly );
	    
	    //run the aggregation
	    
	    weeklyAvgerageFlow.start();
	    weeklyAvgerageFlow.complete();
	    weeklyStdDevFlow.start();
	    weeklyStdDevFlow.complete();
	}

}
