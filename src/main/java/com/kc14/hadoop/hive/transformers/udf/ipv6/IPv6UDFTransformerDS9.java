package com.kc14.hadoop.hive.transformers.udf.ipv6;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collection;

// import javax.xml.bind.DatatypeConverter;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import com.kc14.hadoop.hive.transformers.udf.common.ColumnProjections;
import com.kc14.hadoop.hive.transformers.udf.common.UDFTransformerDS9;
import com.kc14.janvanbesien.com.googlecode.ipv6.IPv6Address;
import com.kc14.janvanbesien.com.googlecode.ipv6.IPv6AddressRange;

public class IPv6UDFTransformerDS9 extends UDFTransformerDS9 {
	
	// Ctor
	public IPv6UDFTransformerDS9 (ColumnProjections colProjections, int numOfBuffers) throws UnknownHostException {
		super (colProjections, numOfBuffers);
	}
	
	public static void main(String[] args) throws IOException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException, ParseException {
		
		// Option Parsing

		Options options = new Options(); // Our own options ...

		options.addOption(Option.builder()
				.longOpt      ("test")
				.desc         ("run test and exit")
				.hasArg       (false)
				.required     (false)
				.build());
		
		// ipv6UDFs = new IPv6UDFsWithCache();
		IPv6UDFs ipv6UDFs = new IPv6UDFs();
		
		Collection<Option> udfOptions = ipv6UDFs.getOptions(); // Options from the UDFs

		for (Option udfOption : udfOptions) {
			options.addOption(udfOption);
		}
		
		CommandLine commandLine = UDFTransformerDS9.parse(args, options);

		// Let's rock

		boolean isRuntest = false;
		if (commandLine.hasOption("test")) isRuntest = true;

		ipv6UDFs.initFrom(commandLine);
		
		IPv6UDFTransformerDS9 udfTransformer = new IPv6UDFTransformerDS9(UDFTransformerDS9.getSelectProjection(commandLine, ipv6UDFs), UDFTransformerDS9.getBuffers(commandLine));

		if (isRuntest) {
			testrun(10_000_000, "223.255.242.97", ipv6UDFs.getIPv6NetworkRanges());
			System.exit(0);
		}
		
		udfTransformer.run();
		
	}
	
	// Run for lookup speed test

	private static void testrun(int lookups, String hostname, IPv6AddressRange[] ipv6NetworkRanges) throws UnknownHostException {
		System.err.format("Starting Lookup with n: %,d ...\n", lookups);
		IPv6Address ipv6Address1 = IPv6UDFs.toIPv6AddressFromStrFurios(hostname);
		System.out.println("ipv6Address1: " + ipv6Address1.toShortString());
		InetAddress ipv6Address2 = InetAddress.getByName("::FFFF:" + hostname);
		System.out.println("ipv6Address2: " + ipv6Address2.getHostAddress());
		for (int i = 1; i <= 10_000_000; ++i) {
			// InetAddress ipv6Address3 = InetAddress.getByName("::FFFF:" + hostname);
			IPv6Address ipv6Address = IPv6UDFs.toIPv6AddressFromStrFurios(hostname);
			int n = IPv6UDFsWithCache.findIdxOfGreatest_IPv6AddressRangeFirst_LesserThanOrEqualTo (ipv6NetworkRanges, ipv6Address);
			if (i % 100_000 == 0) System.err.format("run[%,d]: IPv6: %s, ipv6NetworkRanges[%,d]: [%s]\n", i, ipv6Address.toString(), n, (n >= 0 ? ipv6NetworkRanges[n].toString() : "N/A"));
		}
	}
	
}
