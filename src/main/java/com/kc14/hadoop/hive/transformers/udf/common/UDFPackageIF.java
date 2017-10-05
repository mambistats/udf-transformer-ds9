package com.kc14.hadoop.hive.transformers.udf.common;

import java.lang.Exception;
import java.util.Collection;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;

public interface UDFPackageIF {
	static final String PACKAGE_SEPARATOR_REGEX = "\\."; // An UDF name like Basic.concat_v[1,2,3]
	static final String UDF_PREFIX = "UDF_"; // UDF implementing Methods must start with this prefix
	static final String HIVE_NULL_STR = "\\N";
	
	String getPackageName();

	Collection<Option> getOptions();

	void initFrom(CommandLine commandLine) throws Exception;
	
	UDFMethod getUDF(String udfName, int[] udfCols) throws NoSuchMethodException, SecurityException;
	
	void setInputRow (String[] inputRow);
	
	String[] getInputRow();
	
	String call(UDFMethod udf, String... varargs);
	
	// String call(Object udfPackage, Method udf, String... varargs);
	
}
