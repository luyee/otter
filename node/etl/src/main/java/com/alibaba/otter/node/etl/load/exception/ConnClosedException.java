package com.alibaba.otter.node.etl.load.exception;

public class ConnClosedException extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = -5287085089290828159L;
	public ConnClosedException(String message) {
		super(message);
	}
}
