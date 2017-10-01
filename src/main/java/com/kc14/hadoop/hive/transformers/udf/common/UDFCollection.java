package com.kc14.hadoop.hive.transformers.udf.common;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Collection;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import com.kc14.hadoop.hive.transformers.udf.UDFRegisteredPackage;

/**
 * @author Frank Kemmer
 *
 * Looks up the UDF package for the given UDF function
 * initializes the input row for that package
 * Calls the UDF
 */
public class UDFCollection extends UDFAdapter implements UDFPackageIF {

	UDFRegisteredPackage[] udfPackages = null;
	
	public UDFCollection (String[] udfPackagesToLoadByName) {
		this.udfPackages = new UDFRegisteredPackage[udfPackagesToLoadByName.length];
		for (int i = 0; i < udfPackagesToLoadByName.length; ++i) {
			this.udfPackages[i] = UDFRegisteredPackage.valueOf(udfPackagesToLoadByName[i]);
		}
	}

	@Override
	public Collection<Option> getOptions() {
		Options udfPackagesOptions = new Options();
		for (UDFRegisteredPackage udfPackage : this.udfPackages) {
			for (Option udfOption : udfPackage.getUDFPackageIF().getOptions()) {
				udfPackagesOptions.addOption(udfOption);
			}
		}
		return udfPackagesOptions.getOptions();
	}
	
	@Override
	public void initFrom(CommandLine commandLine) throws FileNotFoundException, UnsupportedEncodingException, IOException {
		for (UDFRegisteredPackage udfPackage : this.udfPackages) {
			udfPackage.getUDFPackageIF().initFrom(commandLine);
		}
	}

	@Override
	public UDFMethod getUDF(String udfName, int[] udfCols) throws NoSuchMethodException, SecurityException {
		String[] split = udfName.split(PACKAGE_SEPARATOR_REGEX);
		try {
			String udfPackageName = split[0];
			String udfMethodName = split[1];
			UDFRegisteredPackage udfPackage = UDFRegisteredPackage.valueOf(udfPackageName);
			return udfPackage.getUDFPackageIF().getUDF(udfMethodName, udfCols);
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
		for (UDFRegisteredPackage udfPackage : this.udfPackages) {
			udfPackage.getUDFPackageIF().setInputRow(inputRow);
		}
	}

}
