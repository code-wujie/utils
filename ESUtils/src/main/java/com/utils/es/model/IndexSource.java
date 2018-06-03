package com.utils.es.model;

public class IndexSource {
	
	private IndexModel index;
	private SourceModel source;
	
	public IndexSource(IndexModel index, SourceModel source) {
		this.index = index;
		this.source = source;
	}

	public IndexModel getIndex() {
		return index;
	}

	public void setIndex(IndexModel index) {
		this.index = index;
	}

	public SourceModel getSource() {
		return source;
	}

	public void setSource(SourceModel source) {
		this.source = source;
	}

	@Override
	public String toString() {
		return "IndexSource [index=" + index + ", source=" + source + "]";
	}
	
}
