package com.kc14.hadoop.hive.transformers.udf;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.Collection;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import com.kc14.hadoop.hive.transformers.udf.common.ColumnProjections;
import com.kc14.hadoop.hive.transformers.udf.common.UDFCollection;
import com.kc14.hadoop.hive.transformers.udf.common.UDFTransformerDS9;

public class UniversalTransformer extends UDFTransformerDS9 {

	public UniversalTransformer(ColumnProjections colProjections, int numOfBuffers) {
		super(colProjections, numOfBuffers);
	}
	
	public static void main(String[] args) throws IOException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException, ParseException, InstantiationException, ClassNotFoundException {
		
		// UniversalTransformer Arguments & Option Parsing

		// First Argument is csv list of UDF packages to use ...
		
		if (args.length < 1) {
			System.err.println("The first argument must be a comma separated list of UDF packages classes to load, e.g. [com.kc14.hadoop.hive.transformers.udf.common.BasicUDFs].");
			System.exit(99);
		}

		String[] udfPackagesToLoadByName = args[0].split(",");

		String[] shiftedArgs = Arrays.copyOfRange(args, 1, args.length);


		// Option Parsing
		
		Options options = new Options(); // Our own options ...
		
		UDFCollection udfCollection = new UDFCollection(udfPackagesToLoadByName);
		
		Collection<Option> udfOptions = udfCollection.getOptions(); // Options from the UDFs

		for (Option udfOption : udfOptions) {
			options.addOption(udfOption);
		}

		CommandLine commandLine = UDFTransformerDS9.parse(shiftedArgs, options);
		
		// Let's rock
		
		udfCollection.initFrom(commandLine);

		UniversalTransformer udfTransformer = new UniversalTransformer(UDFTransformerDS9.getSelectProjection(commandLine, udfCollection), UDFTransformerDS9.getBuffers(commandLine));
		
		udfTransformer.run();

	}

}
