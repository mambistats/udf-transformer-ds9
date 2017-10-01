package com.kc14.hadoop.hive.transformers.udf;

import com.kc14.hadoop.hive.transformers.udf.common.BasicUDFs;
import com.kc14.hadoop.hive.transformers.udf.common.UDFPackageIF;
import com.kc14.hadoop.hive.transformers.udf.ipv6.IPv6UDFs;

public enum UDFRegisteredPackage {
	
	Basic (new BasicUDFs())
	, IPv6  (new IPv6UDFs())
	;

	private UDFPackageIF udfPackage;

	UDFRegisteredPackage(UDFPackageIF udfPackage) {
		this.udfPackage = udfPackage;
	}
	
	public UDFPackageIF getUDFPackageIF() {
		return this.udfPackage;
	}

}
