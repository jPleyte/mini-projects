package io.github.jpleyte.mapreduce.bean;

import java.io.Serializable;

public class Variant implements Serializable {
	private String runId;
	private String chromosome;
	private int positionStart;
	private int positionEnd;
	private String referenceBase;
	private String variantBase;
	private String dbSnpId;
	private String significance;
	
	public String getSignificance() {
		return significance;
	}
	public void setSignificance(String significance) {
		this.significance = significance;
	}

	public String getRunId() {
		return runId;
	}

	public void setRunId(String runId) {
		this.runId = runId;
	}
	public String getChromosome() {
		return chromosome;
	}
	public void setChromosome(String chromosome) {
		this.chromosome = chromosome;
	}
	public int getPositionStart() {
		return positionStart;
	}
	public void setPositionStart(int positionStart) {
		this.positionStart = positionStart;
	}
	public int getPositionEnd() {
		return positionEnd;
	}
	public void setPositionEnd(int positionEnd) {
		this.positionEnd = positionEnd;
	}
	public String getReferenceBase() {
		return referenceBase;
	}
	public void setReferenceBase(String referenceBase) {
		this.referenceBase = referenceBase;
	}
	public String getVariantBase() {
		return variantBase;
	}
	public void setVariantBase(String variantBase) {
		this.variantBase = variantBase;
	}
	public String getDbSnpId() {
		return dbSnpId;
	}
	public void setDbSnpId(String dbSnpId) {
		this.dbSnpId = dbSnpId;
	}
}