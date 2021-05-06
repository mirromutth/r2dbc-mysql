package dev.miku.r2dbc.mysql.authentication;

import static dev.miku.r2dbc.mysql.util.AssertUtils.requireNonNull;

import java.nio.CharBuffer;

import static dev.miku.r2dbc.mysql.constant.Envelopes.TERMINAL;
import dev.miku.r2dbc.mysql.collation.CharCollation;

/**
 * An implementation of {@link MySqlAuthProvider} for type "mysql_clear_password".
 */
public class MySqlClearAuthProvider implements MySqlAuthProvider {

	static final MySqlClearAuthProvider INSTANCE = new MySqlClearAuthProvider();
	
	@Override
	public String getType() {
		return MYSQL_CLEAR_PASSWORD;
	}

	@Override
	public boolean isSslNecessary() {
		return true;
	}

	@Override
	public byte[] authentication(CharSequence password, byte[] salt, CharCollation collation) {
		if (password == null || password.length() <= 0) {
            return new byte[] { TERMINAL };
        }
		requireNonNull(collation, "collation must not be null when password exists");

		return AuthUtils.encodeTerminal(CharBuffer.wrap(password), collation.getCharset());
	}

	@Override
	public MySqlAuthProvider next() {
		return this;
	}

}
