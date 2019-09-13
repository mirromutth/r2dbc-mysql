/*
 * Copyright 2018-2019 the original author or authors.
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

import dev.miku.r2dbc.mysql.internal.AddressUtils;
import dev.miku.r2dbc.mysql.internal.AssertUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.util.annotation.Nullable;

import javax.naming.InvalidNameException;
import javax.naming.ldap.LdapName;
import javax.naming.ldap.Rdn;
import javax.net.ssl.SSLException;
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

/**
 * MySQL hostname verifier, it is NOT an implementation of {@code HostnameVerifier}, because it
 * needs to throw detailed exception which behavior not like {@code HostnameVerifier.verity}.
 */
final class MySqlHostVerifier {

    private static final Logger logger = LoggerFactory.getLogger(MySqlHostVerifier.class);

    private static final String COMMON_NAME = "CN";

    private MySqlHostVerifier() {
    }

    static void accept(String host, SSLSession session) throws SSLException {
        AssertUtils.requireNonNull(host, "host must not be null");
        AssertUtils.requireNonNull(session, "session must not be null");

        Certificate[] certs = session.getPeerCertificates();

        if (certs.length < 1) {
            throw new SSLException(String.format("Certificate for '%s' does not exists", host));
        }

        if (!(certs[0] instanceof X509Certificate)) {
            throw new SSLException(String.format("Certificate for '%s' must be X509Certificate (not javax) rather than %s", host, certs[0].getClass()));
        }

        accepted(host, (X509Certificate) certs[0]);
    }

    private static void accepted(String host, X509Certificate cert) throws SSLException {
        HostType type = determineHostType(host);
        List<San> sans = extractSans(cert);

        if (!sans.isEmpty()) {
            // For self-signed certificate, supports SAN of IP.
            switch (type) {
                case IP_V4:
                    matchIpv4(host, sans);
                    break;
                case IP_V6:
                    matchIpv6(host, sans);
                    break;
                default:
                    // Or just match DNS name?
                    matchDns(host, sans);
                    break;
            }
        } else {
            // RFC 6125, validator must check SAN first, and if SAN exists, then CN should not be checked.
            String cn = extractCn(cert);

            if (cn == null) {
                throw new SSLException(String.format("Certificate for '%s' does not contain the Common Name", host));
            }

            matchCn(host, cn);
        }
    }

    private static void matchIpv4(String ip, List<San> sans) throws SSLPeerUnverifiedException {
        for (San san : sans) {
            // IP must be case sensitive.
            if (San.IP == san.getType() && ip.equals(san.getValue())) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Certificate for '{}' matched by IPv4 value '{}' of the Subject Alternative Names", ip, san.getValue());
                }
                return;
            }
        }

        throw new SSLPeerUnverifiedException(String.format("Certificate for '%s' does not match any of the Subject Alternative Names: %s", ip, sans));
    }

    private static void matchIpv6(String ip, List<San> sans) throws SSLPeerUnverifiedException {
        String host = normaliseIpv6(ip);

        for (San san : sans) {
            // IP must be case sensitive.
            if (san.getType() == San.IP && host.equals(normaliseIpv6(san.getValue()))) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Certificate for '{}' matched by IPv6 value '{}' of the Subject Alternative Names", ip, san.getValue());
                }
                return;
            }
        }

        throw new SSLPeerUnverifiedException(String.format("Certificate for '%s' does not match any of the Subject Alternative Names: %s", ip, sans));
    }

    private static void matchDns(String hostname, List<San> sans) throws SSLPeerUnverifiedException {
        String host = hostname.toLowerCase(Locale.ROOT);

        if (host.isEmpty() || host.charAt(0) == '.' || host.endsWith("..")) {
            // Invalid hostname
            throw new SSLPeerUnverifiedException(String.format("Certificate for '%s' cannot match the Subject Alternative Names because it is invalid name", hostname));
        }

        for (San san : sans) {
            if (san.getType() == San.DNS && matchHost(host, san.getValue().toLowerCase(Locale.ROOT))) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Certificate for '{}' matched by DNS name '{}' of the Subject Alternative Names", host, san.getValue());
                }
                return;
            }
        }

        throw new SSLPeerUnverifiedException(String.format("Certificate for '%s' does not match any of the Subject Alternative Names: %s", hostname, sans));
    }

    private static void matchCn(String hostname, String commonName) throws SSLPeerUnverifiedException {
        String host = hostname.toLowerCase(Locale.ROOT);
        String cn = commonName.toLowerCase(Locale.ROOT);

        if (host.isEmpty() || host.charAt(0) == '.' || host.endsWith("..") || !matchHost(host, cn)) {
            throw new SSLPeerUnverifiedException(String.format("Certificate for '%s' does not match the Common Name: %s", hostname, commonName));
        }

        if (logger.isDebugEnabled()) {
            logger.debug("Certificate for '{}' matched by Common Name '{}'", hostname, commonName);
        }
    }

    /**
     * @param host    must be a string of lower case and it must be validated hostname.
     * @param pattern must be a string of lower case.
     * @return {@code true} if {@code pattern} matching the {@code host}.
     */
    private static boolean matchHost(String host, String pattern) {
        if (pattern.isEmpty() || pattern.charAt(0) == '.' || pattern.endsWith("..")) {
            return false;
        }

        // RFC 2818, 3.1. Server Identity
        // "...Names may contain the wildcard character * which is considered to match any single domain name component or component fragment..."
        // According to this statement, assume that only a single wildcard is legal
        int asteriskIndex = pattern.indexOf('*');

        if (asteriskIndex < 0) {
            // Both are lower case, so no need use equalsIgnoreCase.
            return host.equals(pattern);
        }

        int patternSize = pattern.length();

        if (patternSize == 1) {
            // No one can signature certificate for "*".
            return false;
        }

        if (asteriskIndex > 0) {
            String prefix = pattern.substring(0, asteriskIndex);

            if (!host.startsWith(prefix)) {
                return false;
            }
        }

        int postfixSize = patternSize - asteriskIndex - 1;

        if (postfixSize > 0) {
            String postfix = pattern.substring(asteriskIndex + 1);

            if (!host.endsWith(postfix)) {
                return false;
            }
        }

        int remainderIndex = host.length() - postfixSize;

        if (remainderIndex <= asteriskIndex) {
            // Asterisk must to match least one character.
            // In other words: groups.*.example.com can not match groups..example.com
            return false;
        }

        String remainder = host.substring(asteriskIndex, remainderIndex);
        // Asterisk cannot match across domain name labels.
        return !remainder.contains(".");
    }

    @Nullable
    private static String extractCn(X509Certificate cert) throws SSLException {
        String principal = cert.getSubjectX500Principal().getName(X500Principal.RFC2253);
        LdapName name;

        try {
            name = new LdapName(principal);
        } catch (InvalidNameException e) {
            throw new SSLException(e.getMessage(), e);
        }

        for (Rdn rdn : name.getRdns()) {
            if (rdn.getType().equalsIgnoreCase(COMMON_NAME)) {
                return rdn.getValue().toString();
            }
        }

        return null;
    }

    private static List<San> extractSans(X509Certificate cert) {
        try {
            Collection<List<?>> pairs = cert.getSubjectAlternativeNames();

            if (pairs == null || pairs.isEmpty()) {
                return Collections.emptyList();
            }

            List<San> sans = new ArrayList<>();

            for (List<?> pair : pairs) {
                // Ignore if it is not a pair.
                if (pair == null || pair.size() < 2) {
                    continue;
                }

                Integer type = determineSubjectType(pair.get(0));

                if (type == null) {
                    continue;
                }

                if (San.DNS == type || San.IP == type) {
                    Object value = pair.get(1);

                    if (value instanceof String) {
                        sans.add(new San((String) value, type));
                    } else if (value instanceof byte[]) {
                        // TODO: decode ASN.1 DER form.
                        logger.warn("Certificate contains an ASN.1 DER encoded form in Subject Alternative Names, but DER is unsupported now");
                    } else if (logger.isWarnEnabled()) {
                        logger.warn("Certificate contains an unknown value of Subject Alternative Names: {}", value.getClass());
                    }
                } else {
                    logger.warn("Certificate contains an unknown type of Subject Alternative Names: {}", type);
                }
            }

            return sans;
        } catch (CertificateParsingException ignored) {
            return Collections.emptyList();
        }
    }

    private static String normaliseIpv6(String ip) {
        try {
            return InetAddress.getByName(ip).getHostAddress();
        } catch (UnknownHostException unexpected) {
            return ip;
        }
    }

    private static HostType determineHostType(String hostname) {
        if (AddressUtils.isIpv4(hostname)) {
            return HostType.IP_V4;
        }

        int maxIndex = hostname.length() - 1;
        String host;

        if (hostname.charAt(0) == '[' && hostname.charAt(maxIndex) == ']') {
            host = hostname.substring(1, maxIndex);
        } else {
            host = hostname;
        }

        if (AddressUtils.isIpv6(host)) {
            return HostType.IP_V6;
        }

        return HostType.DNS;
    }

    @Nullable
    private static Integer determineSubjectType(Object type) {
        if (type instanceof Integer) {
            return (Integer) type;
        } else {
            try {
                return Integer.parseInt(type.toString());
            } catch (NumberFormatException e) {
                return null;
            }
        }
    }

    private enum HostType {

        IP_V6,
        IP_V4,
        DNS
    }
}
