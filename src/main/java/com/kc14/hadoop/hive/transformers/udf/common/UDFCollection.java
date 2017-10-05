package com.kc14.hadoop.hive.transformers.udf.common;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

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
	
	Map<String, UDFPackageIF> udfPackages = null;
	
	public UDFCollection (String[] udfPackagesToLoadByName) throws InstantiationException, IllegalAccessException, ClassNotFoundException {
		this.udfPackages = new LinkedHashMap<String, UDFPackageIF>(udfPackagesToLoadByName.length);
		for (String udfPackageToLoadByName: udfPackagesToLoadByName) {
			UDFPackageIF udfPackage = (UDFPackageIF) Class.forName(udfPackageToLoadByName).newInstance();
			String udfPackageName = udfPackage.getPackageName();
			this.udfPackages.put(udfPackageName, udfPackage);
		}
	}

	@Override
	public Collection<Option> getOptions() {
		Options udfPackagesOptions = new Options();
		for (Entry<String, UDFPackageIF> udfPackageEntry : this.udfPackages.entrySet()) {
			for (Option udfOption : udfPackageEntry.getValue().getOptions()) {
				udfPackagesOptions.addOption(udfOption);
			}
		}
		return udfPackagesOptions.getOptions();
	}
	
	@Override
	public void initFrom(CommandLine commandLine) throws Exception {
		for (Entry<String, UDFPackageIF> udfPackageEntry : this.udfPackages.entrySet()) {
			udfPackageEntry.getValue().initFrom(commandLine);
		}
	}

	@Override
	public UDFMethod getUDF(String udfName, int[] udfCols) throws NoSuchMethodException, SecurityException {
		String[] split = udfName.split(PACKAGE_SEPARATOR_REGEX);
		try {
			String udfPackageName = split[0];
			String udfMethodName = split[1];
			UDFPackageIF udfPackage = this.udfPackages.get(udfPackageName);
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
		for (Entry<String, UDFPackageIF> udfPackageEntry : this.udfPackages.entrySet()) {
			udfPackageEntry.getValue().setInputRow(inputRow);
		}
	}

}
