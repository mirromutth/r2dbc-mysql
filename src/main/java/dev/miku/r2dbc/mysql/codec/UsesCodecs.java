package dev.miku.r2dbc.mysql.codec;

/**
 * If a codec implement this interface, it will get a
 * reference to {@link Codecs}
 */
public interface UsesCodecs {
    void setCodecs(Codecs codecs);
}
