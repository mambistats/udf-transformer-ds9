package com.kc14.hadoop.hive.transformers.udf;

import com.kc14.hadoop.hive.transformers.udf.common.BasicUDFs;
import com.kc14.hadoop.hive.transformers.udf.common.UDFPackageIF;
import com.kc14.hadoop.hive.transformers.udf.ipv6.IPv6UDFs;
import com.kc14.hadoop.hive.transformers.udf.udger.UdgerUDFs;

public enum UDFRegisteredPackage {
	
	Basic (new BasicUDFs())
	, IPv6  (new IPv6UDFs())
	, Udger (new UdgerUDFs())
	;

	private UDFPackageIF udfPackage;

	UDFRegisteredPackage(UDFPackageIF udfPackage) {
		this.udfPackage = udfPackage;
	}
	
	public UDFPackageIF getUDFPackageIF() {
		return this.udfPackage;
	}

}
