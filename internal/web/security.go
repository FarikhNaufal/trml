package web

import (
	"crypto/subtle"
	"encoding/json"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

type clientBucket struct {
	tokens         float64
	lastTokenTime  time.Time
	failedPINCount int
	blockedUntil   time.Time
}

type IPRateLimiter struct {
	mu          sync.Mutex
	clients     map[string]*clientBucket
	rateRPM     float64
	burst       float64
	lastCleanup time.Time
}

func NewIPRateLimiter(rateRPM int, burst int) *IPRateLimiter {
	if rateRPM <= 0 {
		rateRPM = 10
	}
	if burst <= 0 {
		burst = 3
	}
	return &IPRateLimiter{
		clients:     make(map[string]*clientBucket),
		rateRPM:     float64(rateRPM),
		burst:       float64(burst),
		lastCleanup: time.Now(),
	}
}

func (limiter *IPRateLimiter) getClientIP(r *http.Request) string {
	if cf := r.Header.Get("CF-Connecting-IP"); cf != "" {
		return strings.TrimSpace(cf)
	}
	if realIP := r.Header.Get("X-Real-IP"); realIP != "" {
		return strings.TrimSpace(realIP)
	}
	forwarded := r.Header.Get("X-Forwarded-For")
	if forwarded != "" {
		parts := strings.Split(forwarded, ",")
		if len(parts) > 0 {
			ip := strings.TrimSpace(parts[0])
			if ip != "" {
				return ip
			}
		}
	}
	ip, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return ip
}

func (limiter *IPRateLimiter) Allow(ip string) (bool, time.Duration) {
	limiter.mu.Lock()
	defer limiter.mu.Unlock()

	now := time.Now()

	// Periodic cleanup every 10 minutes
	if now.Sub(limiter.lastCleanup) > 10*time.Minute {
		for k, b := range limiter.clients {
			if now.Sub(b.lastTokenTime) > 30*time.Minute && now.After(b.blockedUntil) {
				delete(limiter.clients, k)
			}
		}
		limiter.lastCleanup = now
	}

	b, exists := limiter.clients[ip]
	if !exists {
		b = &clientBucket{
			tokens:        limiter.burst,
			lastTokenTime: now,
		}
		limiter.clients[ip] = b
	}

	if now.Before(b.blockedUntil) {
		return false, b.blockedUntil.Sub(now)
	}

	// Token bucket refill: rateRPM per 60 seconds
	elapsed := now.Sub(b.lastTokenTime).Seconds()
	b.lastTokenTime = now
	b.tokens += elapsed * (limiter.rateRPM / 60.0)
	if b.tokens > limiter.burst {
		b.tokens = limiter.burst
	}

	if b.tokens >= 1.0 {
		b.tokens -= 1.0
		return true, 0
	}

	missing := 1.0 - b.tokens
	retryAfterSec := missing / (limiter.rateRPM / 60.0)
	if retryAfterSec < 1.0 {
		retryAfterSec = 1.0
	}
	return false, time.Duration(retryAfterSec) * time.Second
}

func (limiter *IPRateLimiter) RecordFailedPIN(ip string) bool {
	limiter.mu.Lock()
	defer limiter.mu.Unlock()

	now := time.Now()
	b, exists := limiter.clients[ip]
	if !exists {
		b = &clientBucket{
			tokens:        limiter.burst,
			lastTokenTime: now,
		}
		limiter.clients[ip] = b
	}

	b.failedPINCount++
	if b.failedPINCount >= 5 {
		b.blockedUntil = now.Add(5 * time.Minute)
		return true // IP diblokir sementara
	}
	return false
}

func (limiter *IPRateLimiter) ResetFailedPIN(ip string) {
	limiter.mu.Lock()
	defer limiter.mu.Unlock()

	if b, exists := limiter.clients[ip]; exists {
		b.failedPINCount = 0
		b.blockedUntil = time.Time{}
	}
}

func (limiter *IPRateLimiter) IsBlocked(ip string) (bool, time.Duration) {
	limiter.mu.Lock()
	defer limiter.mu.Unlock()

	if b, exists := limiter.clients[ip]; exists {
		now := time.Now()
		if now.Before(b.blockedUntil) {
			return true, b.blockedUntil.Sub(now)
		}
	}
	return false, 0
}

func (s *Server) isPINConfigured() bool {
	return strings.TrimSpace(s.cfg.DashboardPIN) != ""
}

func (s *Server) validatePIN(providedPIN string) bool {
	if !s.isPINConfigured() {
		return true
	}
	expected := []byte(strings.TrimSpace(s.cfg.DashboardPIN))
	actual := []byte(strings.TrimSpace(providedPIN))
	if len(expected) != len(actual) {
		return false
	}
	return subtle.ConstantTimeCompare(expected, actual) == 1
}

// requireAuth middleware memeriksa PIN jika DASHBOARD_PIN diisi di config
func (s *Server) requireAuth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !s.isPINConfigured() {
			next(w, r)
			return
		}

		var ip string
		if s.rateLimiter != nil {
			ip = s.rateLimiter.getClientIP(r)
			if blocked, wait := s.rateLimiter.IsBlocked(ip); blocked {
				seconds := int(wait.Seconds())
				if seconds < 1 {
					seconds = 1
				}
				w.Header().Set("Content-Type", "application/json")
				w.Header().Set("Retry-After", strconv.Itoa(seconds))
				w.WriteHeader(http.StatusTooManyRequests)
				_ = json.NewEncoder(w).Encode(map[string]string{
					"error":   "TOO_MANY_FAILED_ATTEMPTS",
					"message": "Terlalu banyak percobaan PIN yang salah. Akses ditangguhkan selama 5 menit.",
				})
				return
			}
		}

		pin := r.Header.Get("X-TRML-PIN")
		if pin == "" {
			if cookie, err := r.Cookie("trml_pin"); err == nil {
				pin = cookie.Value
			}
		}
		if pin == "" {
			pin = r.URL.Query().Get("pin")
		}

		if !s.validatePIN(pin) {
			if s.rateLimiter != nil && ip != "" && pin != "" {
				s.rateLimiter.RecordFailedPIN(ip)
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnauthorized)
			_ = json.NewEncoder(w).Encode(map[string]string{
				"error":   "UNAUTHORIZED",
				"message": "PIN otentikasi diperlukan atau tidak valid.",
			})
			return
		}

		next(w, r)
	}
}

// aiRateLimit middleware membatasi jumlah panggilan ke endpoint AI per IP
func (s *Server) aiRateLimit(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if s.rateLimiter == nil {
			next(w, r)
			return
		}

		ip := s.rateLimiter.getClientIP(r)
		allowed, waitDuration := s.rateLimiter.Allow(ip)
		if !allowed {
			seconds := int(waitDuration.Seconds())
			if seconds < 1 {
				seconds = 1
			}
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("Retry-After", strconv.Itoa(seconds))
			w.WriteHeader(http.StatusTooManyRequests)
			_ = json.NewEncoder(w).Encode(map[string]string{
				"error":   "RATE_LIMITED",
				"message": "Batas panggilan AI Copilot tercapai (10 req/menit). Silakan tunggu sebentar.",
			})
			return
		}

		next(w, r)
	}
}
