package com.kaltura.live.infra.exception;

public class KalturaInternalException extends RuntimeException {

	private static final long serialVersionUID = -4923449413551219154L;
	
	public KalturaInternalException() { super(); }
	public KalturaInternalException(String message) { super(message); }
	public KalturaInternalException(String message, Throwable cause) { super(message, cause); }
	public KalturaInternalException(Throwable cause) { super(cause); }
	
}
