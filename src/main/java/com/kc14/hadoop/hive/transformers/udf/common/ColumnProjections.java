package com.kc14.hadoop.hive.transformers.udf.common;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ColumnProjections {
	
	public static int colToRowIdx(int col) {
		return col - 1;
	}

	public class ColumnProjection {
		
		int       col     =   0;
		String    udfName =   null;
		UDFMethod udfMethod = null;;
		int[]     udfCols =   null;
		String[]  udfArgs =   null;
		
		private ColumnProjection(int col, String udfName, int[] udfCols) throws NoSuchMethodException, SecurityException {
			super();
			this.col = col;
			this.udfName = udfName;
			if (udfName != null) this.udfMethod = udfs.getUDF(udfName, udfCols);
			else this.udfMethod = null;
			this.udfCols = udfCols;
			if (udfCols != null) this.udfArgs = new String[this.udfCols.length];
			else this.udfArgs = null;
		}

		private void setActualArgs() {
			for (int i = 0; i < this.udfCols.length; ++i) {
				this.udfArgs[i] = udfs.getInputRow()[colToRowIdx(this.udfCols[i])];
			}
		}
		
		public String project() throws IllegalAccessException, IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException {
			if (this.hasUDF()) {
				setActualArgs();
				return udfs.call(udfMethod, udfArgs);
			}
			else {
				return udfs.getInputRow()[colToRowIdx(this.col)];
			}
		}

		public boolean hasUDF() {
			return this.udfName != null;
		}
		
	}

	public static final String UDF_CALL_PATTERN = "(\\w+(\\.\\w+)*)\\[(\\d+(,\\d+)*)\\]"; // E.g.: iphex(12)
	public static final Pattern UDF_PRG = Pattern.compile(UDF_CALL_PATTERN);
	
	private ColumnProjection[] colProjections;
	private UDFPackageIF              udfs;
	
	// Factory
	
	public ColumnProjections (String[] colProjectorArgs, UDFPackageIF udfs) throws NoSuchMethodException, SecurityException {
		this.             udfs = udfs;
		this.colProjections = new ColumnProjection[colProjectorArgs.length];
		int i = 0;
		for (String columnProjectorArg: colProjectorArgs) {
			ColumnProjection colProjection = fromSelectOptionArg(columnProjectorArg);
			colProjections[i] = colProjection;
			++i;
		}
	}
	
	private ColumnProjection fromSelectOptionArg(String columnProjectorArg) throws NoSuchMethodException, SecurityException {
		int    col = 0;
		String udfName = null;
		int[]  udfCols = null;
		try { // First we try to parse a numerical column reference
			col = Integer.parseInt(columnProjectorArg);
		}
		catch (NumberFormatException e) {
			col = 0; // Mark that parsing failed!
		}
		if (col > 0) { // Simple ordinal column reference
			return new ColumnProjection(col, null, null);
		}
		else if (col == 0) { // Try to get the UDF and its arguments, a list of comma separated columns
			Matcher matcher = UDF_PRG.matcher(columnProjectorArg);
			if (matcher.matches()) {
				udfName = matcher.group(1);
				String udfArgsList = matcher.group(3);
				String[] udfArgsArray = udfArgsList.split(",");
				udfCols = new int[udfArgsArray.length];
				for (int i = 0; i < udfCols.length; ++i) {
					udfCols[i] = Integer.parseInt(udfArgsArray[i]);
				}
				return new ColumnProjection(col, udfName, udfCols);
			}
			else throw new IllegalArgumentException("bad UDF call: " + columnProjectorArg);
		}
		else throw new IllegalArgumentException("negative column reference: " + col);
	}
	
	public int size() {
		return this.colProjections.length;
	}
	
	public List<String> projectRow(String[] inputRow) throws IllegalAccessException, IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException {
		List<String> outputRow = new ArrayList<String>(this.colProjections.length);
		udfs.setInputRow(inputRow);
		for (ColumnProjection colProjection: colProjections) {
			outputRow.add(colProjection.project());
		}
		return outputRow;
	}
	
}
