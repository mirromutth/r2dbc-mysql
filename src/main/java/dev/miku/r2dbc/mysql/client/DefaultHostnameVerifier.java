/*
 * Copyright 2018-2020 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package dev.miku.r2dbc.mysql.client;

import dev.miku.r2dbc.mysql.util.AddressUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.naming.InvalidNameException;
import javax.naming.ldap.LdapName;
import javax.naming.ldap.Rdn;
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;
import javax.security.auth.x500.X500Principal;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.cert.Certificate;
import java.security.cert.CertificateParsingException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

import static dev.miku.r2dbc.mysql.util.AssertUtils.requireNonNull;

/**
 * An implementation of {@link HostnameVerifier} for verifying hostname by default.
 */
final class DefaultHostnameVerifier implements HostnameVerifier {

    static final DefaultHostnameVerifier INSTANCE = new DefaultHostnameVerifier();

    private static final Logger logger = LoggerFactory.getLogger(DefaultHostnameVerifier.class);

    private static final boolean LOG_DEBUG = logger.isDebugEnabled();

    private static final String COMMON_NAME = "CN";

    private static final int DNS = 0;

    private static final int IP_V4 = 1;

    private static final int IP_V6 = 2;

    @Override
    public boolean verify(String host, SSLSession session) {
        requireNonNull(host, "host must not be null");
        requireNonNull(session, "session must not be null");

        Certificate[] certs;

        try {
            certs = session.getPeerCertificates();
        } catch (SSLPeerUnverifiedException e) {
            logger.error("Load peer certificates failed", e);
            return false;
        }

        if (certs.length == 0) {
            return false;
        }

        if (!(certs[0] instanceof X509Certificate)) {
            logger.warn("Certificate for '{}' must be X509Certificate (not javax) instead of {}", host,
                certs[0].getClass());
            return false;
        }

        X509Certificate cert = (X509Certificate) certs[0];
        List<San> sans = extractSans(cert);

        if (sans.isEmpty()) {
            // RFC 6125, validator must check SAN first, and if SAN exists, then CN should not be checked.
            return matchCn(host, cert);
        }

        // For self-signed certificate, supports SAN of IP.
        switch (determineHostType(host)) {
            case IP_V4:
                return matchIpv4(host, sans);
            case IP_V6:
                return matchIpv6(host, sans);
        }

        return matchDns(host, sans);
    }

    private static boolean matchIpv4(String ip, List<San> sans) {
        for (San san : sans) {
            // IP must be case sensitive.
            if (San.IP == san.getType() && ip.equals(san.getValue())) {
                if (LOG_DEBUG) {
                    logger.debug("Certificate for '{}' matched IPv4 '{}' of the Subject Alternative Names",
                        ip, san.getValue());
                }
                return true;
            }
        }

        logger.warn("Certificate for '{}' does not match any Subject Alternative Names: {}", ip, sans);

        return false;
    }

    private static boolean matchIpv6(String ip, List<San> sans) {
        String host = normaliseIpv6(ip);

        for (San san : sans) {
            // IP must be case sensitive.
            if (San.IP == san.getType() && host.equals(normaliseIpv6(san.getValue()))) {
                if (LOG_DEBUG) {
                    logger.debug("Certificate for '{}' matched IPv6 '{}' of the Subject Alternative Names",
                        ip, san.getValue());
                }
                return true;
            }
        }

        logger.warn("Certificate for '{}' does not match any Subject Alternative Names: {}", ip, sans);

        return false;
    }

    private static boolean matchDns(String host, List<San> sans) {
        if (host.isEmpty() || host.charAt(0) == '.' || host.endsWith("..")) {
            logger.warn("Certificate for '{}' cannot match because it is invalid", host);
            return false;
        }

        for (San san : sans) {
            if (San.DNS == san.getType() && matchHost(host, san.getValue())) {
                if (LOG_DEBUG) {
                    logger.debug("Certificate for '{}' matched DNS '{}' of the Subject Alternative Names",
                        host, san.getValue());
                }
                return true;
            }
        }

        logger.warn("Certificate for '{}' does not match any Subject Alternative Names: {}", host, sans);

        return false;
    }

    private static boolean matchCn(String host, X509Certificate cert) {
        String principal = cert.getSubjectX500Principal().getName(X500Principal.RFC2253);
        LdapName name;

        try {
            name = new LdapName(principal);
        } catch (InvalidNameException e) {
            logger.error("LDAP name parse failed", e);
            return false;
        }

        String cn = null;

        for (Rdn rdn : name.getRdns()) {
            if (COMMON_NAME.equalsIgnoreCase(rdn.getType())) {
                cn = rdn.getValue().toString();
                break;
            }
        }

        if (cn == null) {
            logger.warn("Certificate for '{}' does not contain the Common Name", host);
            return false;
        }

        if (host.isEmpty() || host.charAt(0) == '.' || host.endsWith("..") || !matchHost(host, cn)) {
            logger.warn("Certificate for '{}' does not match the Common Name: {}", host, cn);
            return false;
        }

        if (LOG_DEBUG) {
            logger.debug("Certificate for '{}' matched by Common Name '{}'", host, cn);
        }

        return true;
    }

    /**
     * Check if a validated hostname match a pattern of RFC SAN DNS.
     *
     * @param host    the validated hostname.
     * @param pattern the pattern.
     * @return if matched.
     */
    private static boolean matchHost(String host, String pattern) {
        if (pattern.isEmpty() || pattern.charAt(0) == '.' || pattern.endsWith("..")) {
            return false;
        }

        // RFC 2818, 3.1. Server Identity
        // "...Names may contain the wildcard character * which is considered to match any single domain
        // name component or component fragment..."
        // According to this statement, assume that only a single wildcard is legal
        int asteriskIndex = pattern.indexOf('*');

        if (asteriskIndex < 0) {
            return host.equalsIgnoreCase(pattern);
        }

        int patternSize = pattern.length();

        if (patternSize == 1) {
            // No one can signature certificate for "*".
            logger.warn("Certificate cannot signature as {} for match all identities", pattern);
            return false;
        }

        int postfixSize = patternSize - asteriskIndex - 1;
        int remainderIndex = host.length() - postfixSize;

        if (remainderIndex <= asteriskIndex) {
            // Asterisk must to match least one character.
            // In other words: groups.*.example.com can not match groups..example.com
            return false;
        }

        String lHost = host.toLowerCase(Locale.ROOT);
        String lPattern = pattern.toLowerCase(Locale.ROOT);

        if ((asteriskIndex > 0 && !lHost.startsWith(lPattern.substring(0, asteriskIndex))) ||
            (postfixSize > 0 && !lHost.endsWith(lPattern.substring(asteriskIndex + 1)))) {
            return false;
        }

        // Asterisk cannot match across domain name labels.
        return !host.substring(asteriskIndex, remainderIndex).contains(".");
    }

    private static List<San> extractSans(X509Certificate cert) {
        Collection<List<?>> pairs;

        try {
            pairs = cert.getSubjectAlternativeNames();
        } catch (CertificateParsingException e) {
            logger.warn("Load Subject Alternative Names from Certificate failed", e);
            return Collections.emptyList();
        }

        if (pairs == null || pairs.isEmpty()) {
            return Collections.emptyList();
        }

        List<San> sans = new ArrayList<>();

        for (List<?> pair : pairs) {
            // Ignore if it is not a pair.
            if (pair == null || pair.size() < 2) {
                continue;
            }

            Object left = pair.get(0);

            if (left == null) {
                continue;
            }

            int type;

            if (left instanceof Integer) {
                type = (Integer) left;
            } else {
                try {
                    type = Integer.parseInt(left.toString());
                } catch (NumberFormatException ignored) {
                    logger.info("Unknown SAN type {}", left);
                    continue;
                }
            }

            if (San.DNS == type || San.IP == type) {
                Object value = pair.get(1);

                if (value instanceof String) {
                    sans.add(new San((String) value, type));
                } else if (value instanceof byte[]) {
                    // TODO: decode ASN.1 DER form.
                    logger.warn("Certificate contains an ASN.1 DER encoded form but DER is unsupported now");
                } else if (logger.isWarnEnabled()) {
                    logger.warn("Certificate contains an unknown value of Subject Alternative Names: {}",
                        value.getClass());
                }
            } else {
                logger.warn("Certificate contains an unknown type of Subject Alternative Names: {}", type);
            }
        }

        return sans;
    }

    private static String normaliseIpv6(String ip) {
        try {
            return InetAddress.getByName(ip).getHostAddress();
        } catch (UnknownHostException ignored) {
            return ip;
        }
    }

    private static int determineHostType(String hostname) {
        if (AddressUtils.isIpv4(hostname)) {
            return IP_V4;
        }

        int maxIndex = hostname.length() - 1;
        String host;

        if (hostname.charAt(0) == '[' && hostname.charAt(maxIndex) == ']') {
            host = hostname.substring(1, maxIndex);
        } else {
            host = hostname;
        }

        if (AddressUtils.isIpv6(host)) {
            return IP_V6;
        }

        return DNS;
    }

    private DefaultHostnameVerifier() {
    }
}
