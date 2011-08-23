package org.recxx.exception;

/**
 * Created by Shaine Ismail.
 * User: sismail
 * Date: 22/08/2011
 * Time: 14:41
 * Copyright SNI-Services ltd
 */
public class PropertiesFileException extends RuntimeException {

    public PropertiesFileException(String s, Throwable throwable) {
        super(s, throwable);
    }

    public PropertiesFileException(String s) {
        super(s);
    }
}
