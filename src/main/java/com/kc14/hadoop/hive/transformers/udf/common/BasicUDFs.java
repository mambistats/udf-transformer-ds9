package com.kc14.hadoop.hive.transformers.udf.common;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.Collections;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;

import com.kc14.hadoop.codec.binary.Hex;
import com.kc14.hadoop.hive.transformers.udf.ipv6.IPv6UDFs;

public class BasicUDFs extends UDFAdapter implements UDFPackageIF {

	private static final String PACKAGE_NAME = "Basic"; 

	@Override
	public String getPackageName() {
		return PACKAGE_NAME;
	}

	@Override
	public Collection<Option> getOptions() {
		return Collections.emptyList();
	}
	
	@Override
	public void initFrom(CommandLine commandLine) throws FileNotFoundException, UnsupportedEncodingException, IOException {
	}
	
	// UDFs

	public String UDF_concat(String s1, String s2) {
		return s1 + "+" + s2;
	}
	
	// Varargs Example
	public String UDF_concat_v(String... args) { // Type of args is String[] (I.e. an-Array of Strings)
		return String.join("+", args);
	}
	
	public String UDF_iphex(String value) throws UnknownHostException {
		return Hex.encodeHex(IPv6UDFs.toIPv6AddressFromStrFurios(value).toByteArray());
	}
	
}
