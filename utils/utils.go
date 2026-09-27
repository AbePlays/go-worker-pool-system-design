package utils

import (
	"net"
	"net/url"
	"strings"
)

func ValidateUrl(raw string) bool {
	u, err := url.ParseRequestURI(raw)
	if err != nil {
		return false
	}

	if u.Scheme != "http" && u.Scheme != "https" {
		return false
	}

	host := u.Hostname()
	if host == "" {
		return false
	}

	if host == "localhost" || strings.HasSuffix(host, ".localhost") {
		return false
	}

	if ip := net.ParseIP(host); ip != nil && isBlockedIP(ip) {
		return false
	}

	return true
}

func isBlockedIP(ip net.IP) bool {
	return ip.IsLoopback() || ip.IsLinkLocalUnicast() || ip.IsMulticast() || ip.IsPrivate()
}
