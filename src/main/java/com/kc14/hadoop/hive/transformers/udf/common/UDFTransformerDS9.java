package com.kc14.hadoop.hive.transformers.udf.common;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.lang.reflect.InvocationTargetException;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public abstract class UDFTransformerDS9 {

	ColumnProjections colProjections;
	int               numOfBuffers;

	// Ctor
	public UDFTransformerDS9 (ColumnProjections colProjections, int numOfBuffers) {
		this.colProjections = colProjections;
		this.numOfBuffers = numOfBuffers;
	}

	// Hive transformer loop
	
	public void run() throws IllegalAccessException, IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException, IOException {
		// Read ipaddr from stdin, get ip network start
		// Output: ip, family, normalized_ip, ipaddrAsLongStr, networt_start
		BufferedReader in =  new BufferedReader(new InputStreamReader(System.in),   this.numOfBuffers * 8192);
		BufferedWriter out = new BufferedWriter(new OutputStreamWriter(System.out), this.numOfBuffers * 4096);

		// Declare local vars here to optimize gc
		String line = null;
		String[] inputRow = null;
		List<String> outputRow = null;

		while ((line = in.readLine()) != null) {
			inputRow = line.split(StaticOptionHolder.inputsep);

			outputRow = this.colProjections.projectRow(inputRow);

			out.write(String.join(StaticOptionHolder.outputsep, outputRow));
			out.newLine();
		}
		// in.close(); // Hive should do that ...
		out.flush();
		out.close(); // ... but we are definitely done and need that to flush the data
	}
	
	private final static String OPTION_SELECT =       "select";
	private final static String OPTION_BUFFERS =      "buffers";
	private final static String OPTION_INPUT_SEP =    "input-sep";
	private final static String OPTION_OUTPUT_SEP =   "output-sep";

	private static final int    DEFAULT_NUM_BUFFERS = 1;

	public static CommandLine parse (String[] args, Options otherOptions) throws IOException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException {

		// Option Parsing
		
		Options options = new Options();
		
		options.addOption(Option.builder()
				.longOpt      (OPTION_SELECT)
				.desc         ("select columns or UDF calls to output.\n")
				.required     (true)
				.hasArgs      () // Unlimited
				.argName      ("col or UDF[col]")
				.build());

		options.addOption(Option.builder()
				.longOpt      (OPTION_BUFFERS)
				.desc         ("number of buffers to use for i/o, defaults to 1 (which is good)")
				.required     (false)
				.hasArg       (true)
				.argName      ("n")
				.numberOfArgs (1)
				.type         (Number.class)
				.build());

		options.addOption(Option.builder()
				.longOpt      (OPTION_INPUT_SEP)
				.desc         ("input separator (default [\\t]")
				.required     (false)
				.hasArg       (true)
				.argName      ("separator")
				.numberOfArgs (1)
				.build());

		options.addOption(Option.builder()
				.longOpt      (OPTION_OUTPUT_SEP)
				.desc         ("output separator (default [\\t]")
				.required     (false)
				.hasArg       (true)
				.argName      ("separator")
				.numberOfArgs (1)
				.build());

		String usageHeader = "Stdin: Line with Hive TSV\n"
				+ "Stdout: Hive TSV as defined by --columns\n";
		
		String usageFooter = "See hive transform for more info.";
		
		// Add options from dervied transformer ...

		for (Option otherOption : otherOptions.getOptions()) {
			options.addOption(otherOption);
		}

		CommandLineParser parser = new DefaultParser();
		HelpFormatter formatter = new HelpFormatter();
		CommandLine commandLine = null;

		try {
			commandLine = parser.parse(options, args);
			if (commandLine.hasOption(OPTION_INPUT_SEP)) StaticOptionHolder.inputsep = commandLine.getOptionValue(OPTION_INPUT_SEP);
			if (commandLine.hasOption(OPTION_OUTPUT_SEP)) StaticOptionHolder.outputsep = commandLine.getOptionValue(OPTION_OUTPUT_SEP);
		} catch (ParseException e) {
			System.err.println(e.getMessage());
			boolean autoUsageYes = true;
			formatter.printHelp("UDFTransformer", usageHeader, options, usageFooter, autoUsageYes);
			System.exit(1);
		}

		return commandLine;
		
	}
	
	public static ColumnProjections getSelectProjection (CommandLine commandLine, UDFPackageIF udfs) throws NoSuchMethodException, SecurityException {
		String[] columnProjectorArgs = commandLine.getOptionValues(OPTION_SELECT);
		return new ColumnProjections (columnProjectorArgs, udfs);
	}

	public static int getBuffers (CommandLine commandLine) throws ParseException {
		return commandLine.hasOption(OPTION_BUFFERS) ? ((Number)commandLine.getParsedOptionValue("buffers")).intValue() : DEFAULT_NUM_BUFFERS ;
	}

}
