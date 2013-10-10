package com.cinarra.data;

import java.io.File;
import java.io.FileFilter;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextOutputFormat;

import cascading.flow.Flow;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.operation.regex.RegexSplitter;
import cascading.operation.text.DateFormatter;
import cascading.operation.text.DateParser;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.SumBy;
import cascading.property.AppProps;
import cascading.scheme.hadoop.TextLine;
import cascading.tap.MultiSourceTap;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tap.hadoop.Lfs;
import cascading.tuple.Fields;

public class DailyEventAggregator {

	public static void main(String[] args) {
		// set the current job jar
		
		JobConf jobConf = new JobConf();
		jobConf.setOutputFormat(TextOutputFormat.class);
		
	    Properties properties = new Properties();
	    AppProps.setApplicationJarClass( properties, DailyEventAggregator.class );
	    AppProps.appProps().buildProperties( jobConf );

	    // these paths can be and need to be adjusted
	    String inputDir = "./src/main/resources";
	    String outputPath = "./output/dailyEventSum";
	    
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
	    
	    //split a line into tokens
		   
	    Pipe assembly = new Each( "eventSum", new Fields( "line" ), regexSplitter,Fields.RESULTS );
	   
	    // remove time component from datetime
	    
	    assembly = new Each( assembly, new Fields( "datetime" ), dateParser,Fields.REPLACE );
	    
	    //format date for output
	    assembly = new Each( assembly, new Fields( "datetime" ), formatter,Fields.REPLACE );
	    
	    // statistics
		 
	    Fields groupingFields = new Fields( "datetime","event" );
	    Fields valueField = new Fields( "value" );
	    Fields sumField = new Fields( "event-total" );
	    // add up all event values grouped by date and event id
	    assembly = new SumBy( assembly, groupingFields, valueField, sumField, int.class );
	   
	    
	    Tap tsSinkTap = new Hfs( new TextLine(), outputPath,SinkMode.REPLACE );
	    
	    // connect the assembly to the source and sink taps
	   
	    Flow eventGroupingFlow = new HadoopFlowConnector( properties ).connect( sourceTap, tsSinkTap, assembly );
	    
	    //run the aggregation
	    
	    eventGroupingFlow.start();
	    eventGroupingFlow.complete();
	}

}
