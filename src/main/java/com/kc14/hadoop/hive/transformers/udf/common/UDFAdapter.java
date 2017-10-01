package com.kc14.hadoop.hive.transformers.udf.common;

import java.lang.reflect.Method;
import java.util.Arrays;

public abstract class UDFAdapter implements UDFPackageIF {
	
	String[] inputRow;
	
	@Override
	public void setInputRow(String[] inputRow) {
		this.inputRow = inputRow;
	}

	@Override
	public String[] getInputRow() {
		return inputRow;
	}

	static Class<?>[] formalIntArg = new Class<?>[] { int.class };
	static Class<?>[] formalStringArg = new Class<?>[] { String.class };
	
	private static Class<?>[] createParameterTypes(int[] udfCols) { // Create formal string parameter list
		Class<?>[] parameterTypes = new Class<?>[udfCols.length];
		Arrays.fill(parameterTypes, String.class);
		return parameterTypes;
	}
	
	private Method getFixedArgsUDF (String udfName, int[] udfCols) throws NoSuchMethodException, SecurityException {
		String methodName = UDF_PREFIX + udfName;
		Class<?>[] parameterTypes = createParameterTypes(udfCols);
		return this.getClass().getDeclaredMethod(methodName, parameterTypes);
	}
	
	private final static Class<?>[] PARAMETER_TYPE_VARARGS = { String[].class };

	private Class<?>[] createParameterTypeVarargs() {
		return PARAMETER_TYPE_VARARGS;
	}

	private Method getVarargsUDF (String udfName, int[] udfCols) throws NoSuchMethodException, SecurityException {
		String methodName = UDF_PREFIX + udfName;
		Class<?>[] parameterType = createParameterTypeVarargs();
		return this.getClass().getDeclaredMethod(methodName, parameterType);
	}

	@Override
	public UDFMethod getUDF (String udfName, int[] udfCols) throws NoSuchMethodException, SecurityException {
		try { // First we try to find a method with fixed parameter list
			return new UDFMethod(this, getFixedArgsUDF(udfName, udfCols));
		}
		catch (NoSuchMethodException e) {
			// System.err.format("Looking for [%s] method with varargs.\n", udfName);
		}
		return new UDFMethod(this, getVarargsUDF(udfName, udfCols)); // If this fails, we didn't find any matching method ...
	}

	// Offer default implementation for reflection call
	
	private Object[] asVarargs(String... varargs) {
		return new Object[] { varargs };
	}
	
	private Object[] asFixedArgs(String... varargs) {
		return varargs; // Implicit cast to Object[]
	}

	@Override
	public String call(UDFMethod udf, String... varargs) {
		
		try {
			if (udf.udfMethod.isVarArgs()) return (String) udf.udfMethod.invoke(udf.udfPackage, asVarargs(varargs)); // One argument! I.e the varargs array!
			else return (String) udf.udfMethod.invoke(udf.udfPackage, asFixedArgs(varargs)); // Arguments get projected to formal parameters
		}
		catch (Exception e) {  // Exceptions should not stop the whole hive query ...
			e.printStackTrace();
			return HIVE_NULL_STR; // ... so we just return a hive null
		}
	}
	
}
