package com.hazelcast.usercode;

import com.hazelcast.core.HazelcastException;

public final class UserCodeException extends HazelcastException {

    public UserCodeException() {
        super("UserCode failed.");
    }

    public UserCodeException(String message) {
        super(message);
    }

    public UserCodeException(String message, Throwable cause) {
        super(message, cause);
    }

    public UserCodeException(Throwable cause) {
        super("UserCode failed (see inner exception).", cause);
    }
}
