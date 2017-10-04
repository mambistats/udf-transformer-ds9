package com.kc14.hadoop.hive.transformers.udf.common;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

/**
 * @author Frank Kemmer
 *
 * Looks up the UDF package for the given UDF function
 * initializes the input row for that package
 * Calls the UDF
 */
public class UDFCollection extends UDFAdapter implements UDFPackageIF {
	
	private static final String PACKAGE_NAME = "UDFCollection"; 

	@Override
	public String getPackageName() {
		return PACKAGE_NAME;
	}

	// UDFRegisteredPackage[] udfPackages = null;
	
	UDFPackageIF[] udfPackageList = null;
	Map<String, UDFPackageIF> udfPackageLookup = null;
	
	public UDFCollection (String[] udfPackagesToLoadByName) throws InstantiationException, IllegalAccessException, ClassNotFoundException {
		this.udfPackageList = new UDFPackageIF[udfPackagesToLoadByName.length];
		this.udfPackageLookup = new HashMap<String, UDFPackageIF>(udfPackagesToLoadByName.length);
		for (int i = 0; i < udfPackagesToLoadByName.length; ++i) {
			UDFPackageIF udfPackage = (UDFPackageIF) Class.forName(udfPackagesToLoadByName[i]).newInstance();
			this.udfPackageList[i] = udfPackage;
			String udfPackageName = udfPackage.getPackageName();
			this.udfPackageLookup.put(udfPackageName, udfPackage);
		}
	}

	@Override
	public Collection<Option> getOptions() {
		Options udfPackagesOptions = new Options();
		for (UDFPackageIF udfPackage : this.udfPackageList) {
			for (Option udfOption : udfPackage.getOptions()) {
				udfPackagesOptions.addOption(udfOption);
			}
		}
		return udfPackagesOptions.getOptions();
	}
	
	@Override
	public void initFrom(CommandLine commandLine) throws FileNotFoundException, UnsupportedEncodingException, IOException {
		for (UDFPackageIF udfPackage : this.udfPackageList) {
			udfPackage.initFrom(commandLine);
		}
	}

	@Override
	public UDFMethod getUDF(String udfName, int[] udfCols) throws NoSuchMethodException, SecurityException {
		String[] split = udfName.split(PACKAGE_SEPARATOR_REGEX);
		try {
			String udfPackageName = split[0];
			String udfMethodName = split[1];
			UDFPackageIF udfPackage = this.udfPackageLookup.get(udfPackageName);
			return udfPackage.getUDF(udfMethodName, udfCols);
		}
		catch (ArrayIndexOutOfBoundsException e) {
			System.err.format("UDF [%s]: Bad Format. Is the package name missing?", udfName);
			System.err.println();
			throw e;
		}
	}

	@Override
	public void setInputRow(String[] inputRow) {
		super.setInputRow(inputRow);
		for (UDFPackageIF udfPackage : this.udfPackageList) {
			udfPackage.setInputRow(inputRow);
		}
	}

}
