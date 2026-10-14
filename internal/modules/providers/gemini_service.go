package providers

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	crand "crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"html"
	"io"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"gemini-web-to-api/internal/commons/configs"
	"gemini-web-to-api/internal/commons/utils"

	"github.com/google/uuid"
	"github.com/imroc/req/v3"
	"go.uber.org/zap"
)

type Client struct {
	httpClient   *req.Client
	cookies      *CookieStore
	at           string
	cookieHeader string // full Cookie header string built by refreshSessionToken, used in GenerateContent
	pushID       string
	buildLabel   string
	sessionID    string
	language     string
	mu           sync.RWMutex // protects: at, healthy, cookieHeader, pushID, buildLabel, sessionID, language
	healthy      bool
	log          *zap.Logger

	autoRefresh         bool
	refreshInterval     time.Duration
	stopRefresh         chan struct{}
	maxRetries          int
	cachedModels        []ModelInfo
	defaultTemporary    bool
	cookieSyncFile      string
	cookieSyncMTime     time.Time
	cookieSyncSeenMTime time.Time
	cookieSyncPending   bool
	sessionRefreshMu    sync.Mutex
	chatLocks           sync.Map
}

type CookieStore struct {
	Secure1PSID   string    `json:"__Secure-1PSID"`
	Secure1PSIDTS string    `json:"__Secure-1PSIDTS"`
	UpdatedAt     time.Time `json:"updated_at"`
	mu            sync.RWMutex
}

const (
	defaultRefreshIntervalMinutes = 30
)

var (
	accessTokenRegex         = regexp.MustCompile(`"SNlM0e":"([^"]+)"`)
	accessTokenFallbackRegex = regexp.MustCompile(`\["SNlM0e","([^"]+)"\]`)
	pushIDRegex              = regexp.MustCompile(`"qKIAYe":"([^"]+)"`)
	buildLabelRegex          = regexp.MustCompile(`"cfb2h":"([^"]+)"`)
	sessionIDRegex           = regexp.MustCompile(`"FdrFJe":"([^"]+)"`)
	languageRegex            = regexp.MustCompile(`"TuX5cc":"([^"]+)"`)
	modelIDRegex             = regexp.MustCompile(`gemini-[a-zA-Z0-9.-]+`)
	validModelPrefixRegex    = regexp.MustCompile(`^gemini-(\d|advanced)`)
	imageURLRegex            = regexp.MustCompile(`(?i)(?:https?:)?//[^\s"'<>\\]+|(?:[a-z0-9.-]+\.)?googleusercontent\.com/[^\s"'<>\\]+`)
)

func NewClient(cfg *configs.Config, log *zap.Logger) *Client {
	cookies := &CookieStore{
		Secure1PSID:   cfg.Gemini.Secure1PSID,
		Secure1PSIDTS: cfg.Gemini.Secure1PSIDTS,
		UpdatedAt:     time.Now(),
	}

	client := req.NewClient().
		SetTimeout(10 * time.Minute).
		SetCommonHeaders(DefaultHeaders)

	refreshIntervalMinutes := cfg.Gemini.RefreshInterval
	if refreshIntervalMinutes <= 0 {
		refreshIntervalMinutes = defaultRefreshIntervalMinutes
	}

	return &Client{
		httpClient:       client,
		cookies:          cookies,
		autoRefresh:      true,
		refreshInterval:  time.Duration(refreshIntervalMinutes) * time.Minute,
		stopRefresh:      make(chan struct{}),
		maxRetries:       cfg.Gemini.MaxRetries,
		log:              log,
		defaultTemporary: cfg.Gemini.Temporary,
		cookieSyncFile:   cfg.Gemini.CookieSyncFile,
	}
}

func (c *Client) syncCookiesFromFile() (bool, error) {
	if strings.TrimSpace(c.cookieSyncFile) == "" {
		return false, nil
	}
	info, err := os.Stat(c.cookieSyncFile)
	if err != nil {
		return false, err
	}
	c.mu.RLock()
	unchanged := !c.cookieSyncSeenMTime.IsZero() && !info.ModTime().After(c.cookieSyncSeenMTime)
	pending := c.cookieSyncPending
	c.mu.RUnlock()
	if unchanged {
		return pending, nil
	}
	data, err := os.ReadFile(c.cookieSyncFile)
	if err != nil {
		return false, err
	}
	var values map[string]string
	if err := json.Unmarshal(data, &values); err != nil {
		var wrapper struct {
			Cookies []struct {
				Name  string `json:"name"`
				Value string `json:"value"`
			} `json:"cookies"`
		}
		if wrapperErr := json.Unmarshal(data, &wrapper); wrapperErr != nil {
			return false, fmt.Errorf("decode %s: %w", c.cookieSyncFile, err)
		}
		values = make(map[string]string, len(wrapper.Cookies))
		for _, cookie := range wrapper.Cookies {
			values[cookie.Name] = cookie.Value
		}
	}
	psid := cleanCookie(values["__Secure-1PSID"])
	psidts := cleanCookie(values["__Secure-1PSIDTS"])
	if psid == "" || psidts == "" {
		return false, fmt.Errorf("%s lacks required Gemini cookies", c.cookieSyncFile)
	}

	c.cookies.mu.Lock()
	changed := c.cookies.Secure1PSID != psid || c.cookies.Secure1PSIDTS != psidts
	c.cookies.Secure1PSID = psid
	c.cookies.Secure1PSIDTS = psidts
	c.cookies.UpdatedAt = info.ModTime()
	c.cookies.mu.Unlock()
	c.mu.Lock()
	c.cookieSyncSeenMTime = info.ModTime()
	c.cookieSyncPending = pending || changed
	needsRefresh := c.cookieSyncPending
	c.mu.Unlock()
	if changed {
		c.httpClient.SetCommonCookies(c.cookies.ToHTTPCookies()...)
		c.log.Info("Synced Gemini cookies from browser file")
	}
	return needsRefresh, nil
}

func (c *Client) markCookieSyncApplied() {
	c.mu.Lock()
	c.cookieSyncMTime = c.cookieSyncSeenMTime
	c.cookieSyncPending = false
	c.mu.Unlock()
}

func (c *Client) ensureCurrentBrowserSession() error {
	c.sessionRefreshMu.Lock()
	defer c.sessionRefreshMu.Unlock()

	needsRefresh, err := c.syncCookiesFromFile()
	if err != nil {
		return err
	}
	if !needsRefresh {
		return nil
	}
	if err := c.refreshSessionTokenUnlocked(); err != nil {
		return err
	}
	c.markCookieSyncApplied()
	return nil
}

func (c *Client) Init(ctx context.Context) error {
	if _, err := c.syncCookiesFromFile(); err != nil {
		return fmt.Errorf("sync browser cookies: %w", err)
	}

	// Clean cookies
	c.cookies.Secure1PSID = cleanCookie(c.cookies.Secure1PSID)
	configPSIDTS := cleanCookie(c.cookies.Secure1PSIDTS) // Save original config value
	c.cookies.Secure1PSIDTS = configPSIDTS

	// Check if we should use cached cookies or clear cache
	if c.cookies.Secure1PSID != "" {
		cachedTS, err := c.LoadCachedCookies()

		// If config has a new PSIDTS that differs from cache, clear cache and use config
		if configPSIDTS != "" && cachedTS != "" && configPSIDTS != cachedTS {
			_ = c.ClearCookieCache()
			// Keep using the config value (already set above)
		} else if err == nil && cachedTS != "" && configPSIDTS == "" {
			// Only use cache if config doesn't provide PSIDTS
			c.cookies.Secure1PSIDTS = cachedTS
			c.log.Info("Loaded __Secure-1PSIDTS from cache")
		}
	}

	// Obtain PSIDTS via rotation if missing
	if c.cookies.Secure1PSID != "" && c.cookies.Secure1PSIDTS == "" {
		c.log.Info("Only __Secure-1PSID provided, attempting to obtain __Secure-1PSIDTS via rotation...")
		if err := c.RotateCookies(); err != nil {
			c.log.Info("Rotation failed, proceeding with just __Secure-1PSID (might fail)", zap.String("error", err.Error()))
		} else {
			c.log.Info("Successfully obtained __Secure-1PSIDTS via rotation")
		}
	}

	// Populate cookies
	c.httpClient.SetCommonCookies(c.cookies.ToHTTPCookies()...)

	// Get SNlM0e token
	err := c.refreshSessionToken()
	if err != nil {
		c.log.Debug("Initial session token fetch failed, attempting cookie rotation", zap.Error(err))
		// Try to rotate cookies and retry
		if rotErr := c.RotateCookies(); rotErr == nil {
			c.log.Debug("Cookie rotation succeeded, retrying session token fetch")
			err = c.refreshSessionToken()
		} else {
			c.log.Debug("Cookie rotation failed", zap.Error(rotErr))
		}
	}

	if err != nil {
		return err
	}
	c.markCookieSyncApplied()

	// Save the valid cookies to cache immediately after successful init
	_ = c.SaveCachedCookies()

	c.log.Info("✅ Gemini client initialized successfully")

	// 5. Start auto-refresh in background
	if c.autoRefresh {
		go c.startAutoRefresh()
	}

	return nil
}

func (c *Client) refreshSessionToken() error {
	c.sessionRefreshMu.Lock()
	defer c.sessionRefreshMu.Unlock()
	if err := c.refreshSessionTokenUnlocked(); err != nil {
		return err
	}
	c.markCookieSyncApplied()
	return nil
}

func (c *Client) refreshSessionTokenUnlocked() error {
	// 1. Initial hit to google.com to get extra cookies (NID, etc)
	tmpClient := req.NewClient().
		SetTimeout(30 * time.Second).
		SetUserAgent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

	resp1, err := tmpClient.R().Get("https://www.google.com/")
	extraCookies := ""
	if err == nil {
		parts := []string{}
		for _, ck := range resp1.Cookies() {
			parts = append(parts, fmt.Sprintf("%s=%s", ck.Name, ck.Value))
			// Also sync to main client
			c.httpClient.SetCommonCookies(ck)
		}
		if len(parts) > 0 {
			extraCookies = strings.Join(parts, "; ") + "; "
		}
	}

	// 2. Prepare full cookie string from one consistent snapshot.
	c.cookies.mu.RLock()
	psid := c.cookies.Secure1PSID
	psidts := c.cookies.Secure1PSIDTS
	c.cookies.mu.RUnlock()
	cookieStr := fmt.Sprintf("%s__Secure-1PSID=%s; __Secure-1PSIDTS=%s",
		extraCookies, psid, psidts)

	commonHeaders := map[string]string{
		"Accept":                    "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
		"Accept-Language":           "en-US,en;q=0.9",
		"Cache-Control":             "max-age=0",
		"Origin":                    "https://gemini.google.com",
		"Sec-Ch-Ua":                 `"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"`,
		"Sec-Ch-Ua-Mobile":          "?0",
		"Sec-Ch-Ua-Platform":        `"Windows"`,
		"Sec-Fetch-Dest":            "document",
		"Sec-Fetch-Mode":            "navigate",
		"Sec-Fetch-Site":            "none",
		"Sec-Fetch-User":            "?1",
		"Upgrade-Insecure-Requests": "1",
		"X-Same-Domain":             "1",
		"User-Agent":                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
	}

	hClient := &http.Client{
		Timeout: 30 * time.Second,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return nil // follow redirects
		},
	}

	// Helper to merge cookies into a map to avoid duplicates
	mergeCookies := func(baseStr string, newCks []*http.Cookie) string {
		m := make(map[string]string)
		for _, part := range strings.Split(baseStr, ";") {
			p := strings.TrimSpace(part)
			if p == "" {
				continue
			}
			kv := strings.SplitN(p, "=", 2)
			if len(kv) == 2 {
				m[kv[0]] = kv[1]
			}
		}
		for _, ck := range newCks {
			m[ck.Name] = ck.Value
		}
		res := []string{}
		for k, v := range m {
			res = append(res, fmt.Sprintf("%s=%s", k, v))
		}
		return strings.Join(res, "; ")
	}

	req1, _ := http.NewRequest("GET", "https://gemini.google.com/?hl=en", nil)
	for k, v := range commonHeaders {
		req1.Header.Set(k, v)
	}
	req1.Header.Set("Cookie", cookieStr)
	resp1_direct, _ := hClient.Do(req1)
	if resp1_direct != nil {
		cookieStr = mergeCookies(cookieStr, resp1_direct.Cookies())
		for _, ck := range resp1_direct.Cookies() {
			c.httpClient.SetCommonCookies(ck)
		}
		resp1_direct.Body.Close()
	}

	// 2. The main INIT hit
	req2, _ := http.NewRequest("GET", EndpointInit+"?hl=en", nil)
	for k, v := range commonHeaders {
		req2.Header.Set(k, v)
	}
	req2.Header.Set("Sec-Fetch-Site", "same-origin")
	req2.Header.Set("Cookie", cookieStr)
	req2.Header.Set("Referer", "https://gemini.google.com/")
	req2.Header.Set("Accept-Encoding", "gzip, deflate, br")

	resp, err := hClient.Do(req2)
	if err != nil {
		return fmt.Errorf("failed to reach gemini app: %w", err)
	}
	defer resp.Body.Close()

	// Dump for debugging if it fails
	// reqDump, _ := httputil.DumpRequestOut(req2, false)
	// respDump, _ := httputil.DumpResponse(resp, false)

	var bodyReader io.ReadCloser = resp.Body
	if strings.Contains(resp.Header.Get("Content-Encoding"), "gzip") {
		gz, err := gzip.NewReader(resp.Body)
		if err == nil {
			bodyReader = gz
			defer gz.Close()
		}
	}

	bodyBytes, _ := io.ReadAll(bodyReader)
	body := string(bodyBytes)

	// Merge cookies from the init response into cookieStr
	cookieStr = mergeCookies(cookieStr, resp.Cookies())

	matches := accessTokenRegex.FindStringSubmatch(body)
	if len(matches) < 2 {
		matches = accessTokenFallbackRegex.FindStringSubmatch(body)
		if len(matches) < 2 {
			errMsg := "authentication failed: SNlM0e not found"
			if strings.Contains(body, "Sign in") || strings.Contains(body, "login") {
				errMsg = "authentication failed: cookies invalid. Please provide __Secure-1PSIDTS in addition to __Secure-1PSID"
			}
			c.log.Info(errMsg)
			return fmt.Errorf("%s", errMsg)
		}
	}

	pushID := "feeds/mcudyrk2a4khkz"
	if pushMatches := pushIDRegex.FindStringSubmatch(body); len(pushMatches) >= 2 {
		pushID = pushMatches[1]
	}
	buildLabel := ""
	if buildMatches := buildLabelRegex.FindStringSubmatch(body); len(buildMatches) >= 2 {
		buildLabel = buildMatches[1]
	}
	sessionID := ""
	if sessionMatches := sessionIDRegex.FindStringSubmatch(body); len(sessionMatches) >= 2 {
		sessionID = sessionMatches[1]
	}
	language := "en"
	if langMatches := languageRegex.FindStringSubmatch(body); len(langMatches) >= 2 {
		language = langMatches[1]
	}

	c.mu.Lock()
	c.at = matches[1]
	c.cookieHeader = cookieStr // save full cookie string for use in GenerateContent
	c.pushID = pushID
	c.buildLabel = buildLabel
	c.sessionID = sessionID
	c.language = language
	c.healthy = true
	c.mu.Unlock()

	// Update dynamic models from the same initialization body
	c.refreshModels(body)

	return nil
}

func (c *Client) refreshModels(body string) {
	var newModels []ModelInfo
	now := time.Now().Unix()

	matches := modelIDRegex.FindAllString(body, -1)

	uniqueIDs := make(map[string]bool)
	for _, id := range matches {
		id = strings.Trim(id, `\"' `)
		if !uniqueIDs[id] && len(id) > 10 && validModelPrefixRegex.MatchString(id) {
			uniqueIDs[id] = true
			newModels = append(newModels, ModelInfo{
				ID:       id,
				Created:  now,
				OwnedBy:  "google",
				Provider: "gemini",
			})
		}
	}

	c.mu.Lock()
	c.cachedModels = newModels
	c.mu.Unlock()

	if len(newModels) == 0 {
		c.log.Warn("⚠️ No models found in Gemini Web response. Please check your cookies or connection.")
	} else {
		ids := make([]string, 0, len(newModels))
		for _, m := range newModels {
			ids = append(ids, m.ID)
		}
		c.log.Info("🔄 Refreshed available models from Gemini Web", zap.Int("count", len(newModels)), zap.Strings("models", ids))
	}
}

// startAutoRefresh periodically refreshes the PSIDTS cookie
func (c *Client) startAutoRefresh() {
	ticker := time.NewTicker(c.refreshInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			c.log.Debug("Starting scheduled cookie refresh")
			rotateErr := c.RotateCookies()
			if rotateErr != nil {
				// Check if it's a 401/403 (cookies fully expired) — no point retrying session token
				isCookieExpired := strings.Contains(rotateErr.Error(), "status 401") ||
					strings.Contains(rotateErr.Error(), "status 403")

				if isCookieExpired {
					c.log.Error("Cookies have expired — please update GEMINI_1PSID and GEMINI_1PSIDTS in .env",
						zap.Error(rotateErr),
						zap.String("action", "Visit https://gemini.google.com → F12 → Application → Cookies"),
					)
					c.mu.Lock()
					c.healthy = false
					c.mu.Unlock()
					continue
				}

				// RotateCookies failed but NOT due to expired cookies (Google may not return new cookie every time)
				// Fallback: try to refresh the session token (SNlM0e/at) to keep client alive
				c.log.Warn("Cookie rotation failed, falling back to session token refresh", zap.Error(rotateErr))
				if sessionErr := c.refreshSessionToken(); sessionErr != nil {
					// Both methods failed — mark client as unhealthy so callers know
					c.log.Error("Session token refresh also failed, marking client unhealthy",
						zap.NamedError("rotation_error", rotateErr),
						zap.NamedError("session_error", sessionErr),
					)
					c.mu.Lock()
					c.healthy = false
					c.mu.Unlock()
				} else {
					c.log.Info("Session token refreshed successfully after rotation failure")
					// Ensure client is marked healthy since session token is valid
					c.mu.Lock()
					c.healthy = true
					c.mu.Unlock()
				}
			} else {
				// Rotation succeeded — also refresh session token to keep SNlM0e/at up to date
				if sessionErr := c.refreshSessionToken(); sessionErr != nil {
					c.log.Warn("Cookie rotated but session token refresh failed", zap.Error(sessionErr))
				} else {
					c.log.Info("Cookie and session token refreshed successfully")
				}
			}
		case <-c.stopRefresh:
			return
		}
	}
}

func (c *Client) RotateCookies() error {
	c.cookies.mu.Lock()
	defer c.cookies.mu.Unlock()

	// Prepare cookies for rotation request
	// NOTE: We access fields directly instead of using ToHTTPCookies() to avoid recursive locking (deadlock)
	parts := []string{}
	if c.cookies.Secure1PSID != "" {
		parts = append(parts, fmt.Sprintf("__Secure-1PSID=%s", c.cookies.Secure1PSID))
	}
	if c.cookies.Secure1PSIDTS != "" {
		parts = append(parts, fmt.Sprintf("__Secure-1PSIDTS=%s", c.cookies.Secure1PSIDTS))
	}
	cookieStr := strings.Join(parts, "; ")

	// Payload must be exactly this string
	strBody := `[000,"-0000000000000000000"]`
	req, _ := http.NewRequest("POST", EndpointRotateCookies, strings.NewReader(strBody))

	req.Header.Set("Content-Type", "application/json")
	// Google often blocks requests with default Go-http-client User-Agent
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
	req.Header.Set("Cookie", cookieStr)

	c.log.Debug("Sending rotation request", zap.String("url", EndpointRotateCookies))
	hClient := &http.Client{Timeout: 5 * time.Second}
	resp, err := hClient.Do(req)
	if err != nil {
		// Log as Info to avoid scary stacktraces in development mode for expected auth failures
		c.log.Info("Rotation request failed (network/auth issue)", zap.String("error", err.Error()))
		return fmt.Errorf("failed to call rotation endpoint: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		c.log.Info("Rotation failed (likely invalid __Secure-1PSID)", zap.Int("status", resp.StatusCode))
		return fmt.Errorf("rotation failed with status %d", resp.StatusCode)
	}

	// Extract new PSIDTS from Set-Cookie headers
	found := false
	for _, cookie := range resp.Cookies() {
		if cookie.Name == "__Secure-1PSIDTS" {
			c.cookies.Secure1PSIDTS = cookie.Value
			c.cookies.UpdatedAt = time.Now()
			found = true
			// Save the new cookie to cache immediately
			_ = c.SaveCachedCookies()
		}
		// Sync to req/v3 client for future calls
		c.httpClient.SetCommonCookies(cookie)
	}

	if found {
		c.log.Info("Cookie rotated successfully", zap.Time("updated_at", c.cookies.UpdatedAt))
	} else {
		// Google returns 200 but omits a new cookie when the existing one is still valid — not an error
		c.log.Debug("No new __Secure-1PSIDTS issued; existing cookie is still valid")
	}
	return nil
}

func (c *Client) GetCookies() *CookieStore {
	c.cookies.mu.RLock()
	defer c.cookies.mu.RUnlock()

	return &CookieStore{
		Secure1PSID:   c.cookies.Secure1PSID,
		Secure1PSIDTS: c.cookies.Secure1PSIDTS,
		UpdatedAt:     c.cookies.UpdatedAt,
	}
}

func (c *Client) GenerateContent(ctx context.Context, prompt string, options ...GenerateOption) (*Response, error) {
	return c.generateContent(ctx, prompt, nil, options...)
}

func (c *Client) generateContent(ctx context.Context, prompt string, metadata *SessionMetadata, options ...GenerateOption) (*Response, error) {
	if err := c.ensureCurrentBrowserSession(); err != nil {
		return nil, fmt.Errorf("refresh browser session: %w", err)
	}
	config := &GenerateConfig{}
	for _, opt := range options {
		opt(config)
	}

	// Default to first available model if not set or "gemini-pro"
	c.mu.RLock()
	if config.Model == "" || config.Model == "gemini-pro" {
		if len(c.cachedModels) > 0 {
			config.Model = c.cachedModels[0].ID
		}
	}

	requestedModel := config.Model
	resolvedModel := requestedModel
	found := false
	if _, ok := webModelIDs[requestedModel]; ok {
		// Web-catalog model with a known internal hash: bypass the account catalog.
		found = true
	} else if strings.HasPrefix(requestedModel, "gemini-selector-") {
		found = true
	} else {
		resolvedModel, found = resolveAvailableModel(requestedModel, c.cachedModels)
	}
	config.Model = resolvedModel
	at := c.at
	cookieHdr := c.cookieHeader
	buildLabel := c.buildLabel
	sessionID := c.sessionID
	language := c.language
	c.mu.RUnlock()
	if language == "" {
		language = "en"
	}

	if !found && requestedModel != "" {
		return nil, fmt.Errorf("model '%s' is not supported or not available. Available models: %v", requestedModel, c.ListModelsIDs())
	}

	if at == "" {
		return nil, errors.New("client not initialized")
	}

	uploadedFiles, err := c.uploadRequestFiles(ctx, config, cookieHdr)
	if err != nil {
		return nil, err
	}

	requestID := strings.ToUpper(uuid.NewString())
	_, modelSurface := modelSelectorFor(resolvedModel)
	inner := buildGenerateInner(prompt, uploadedFiles, config.Model, language, requestID, c.defaultTemporary, metadata)

	innerJSON, _ := json.Marshal(inner)
	outer := []interface{}{nil, string(innerJSON)}
	outerJSON, _ := json.Marshal(outer)

	// Encode form body manually to have full control over the request
	formValues := url.Values{}
	formValues.Set("at", at)
	formValues.Set("f.req", string(outerJSON))
	formBody := formValues.Encode()

	queryValues := url.Values{}
	queryValues.Set("at", at)
	if len(uploadedFiles) > 0 {
		queryValues.Set("hl", language)
		queryValues.Set("_reqid", fmt.Sprintf("%d", rand.Intn(90000)+10000))
		queryValues.Set("rt", "c")
		if buildLabel != "" {
			queryValues.Set("bl", buildLabel)
		}
		if sessionID != "" {
			queryValues.Set("f.sid", sessionID)
		}
	}
	generateURL := EndpointGenerate + "?" + queryValues.Encode()

	maxAttempts := c.maxRetries
	if maxAttempts <= 0 {
		maxAttempts = 1
	}

	// Use a plain http.Client to avoid cookie accumulation issues with the req library
	plainClient := &http.Client{Timeout: 5 * time.Minute}

	totalStart := time.Now()

	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if attempt > 1 {
			backoff := time.Duration(1<<uint(attempt-2)) * time.Second
			c.log.Warn("Retrying GenerateContent",
				zap.Int("attempt", attempt),
				zap.Int("max_attempts", maxAttempts),
				zap.Duration("backoff", backoff),
				zap.Error(lastErr),
			)
			select {
			case <-time.After(backoff):
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}

		httpStart := time.Now()

		httpReq, err := http.NewRequestWithContext(ctx, "POST", generateURL, strings.NewReader(formBody))
		if err != nil {
			lastErr = fmt.Errorf("failed to build generate request: %w", err)
			continue
		}
		httpReq.Header.Set("Content-Type", "application/x-www-form-urlencoded;charset=utf-8")
		httpReq.Header.Set("Origin", "https://gemini.google.com")
		httpReq.Header.Set("Referer", "https://gemini.google.com/")
		httpReq.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
		httpReq.Header.Set("X-Same-Domain", "1")
		httpReq.Header.Set("X-Goog-AuthUser", AuthUser)
		if len(uploadedFiles) > 0 {
			httpReq.Header.Set("x-goog-ext-525005358-jspb", fmt.Sprintf(`["%s",1]`, requestID))
			modelSurface = 1
		}
		modeFlag := 0
		if c.defaultTemporary {
			modeFlag = 1
		}
		modelID, modelSelector := modelSelectorFor(resolvedModel)
		traceID := strings.ToUpper(uuid.NewString())
		clientVersion := "boq_gemini-web-uiserver_20261012.19_p1"
		extHeader := fmt.Sprintf(
			`[1,null,null,null,"%s",null,null,%d,[4,5,6,8,16,19,20,4,5,6,8,16,19,20],null,null,2,null,null,%d,%d,"%s",["%s"],null,[[41,598400000],[%d,12000000]]]`,
			modelID, modeFlag, modelSelector, modelSurface, traceID, clientVersion, time.Now().Unix(),
		)
		httpReq.Header.Set("x-goog-ext-525001261-jspb", extHeader)
		httpReq.Header.Set("x-goog-ext-73010989-jspb", "[0]")
		httpReq.Header.Set("x-goog-ext-73010990-jspb", "[0,0,0]")
		httpReq.Header.Set("sec-ch-ua", `"Not;A=Brand";v="8", "Chromium";v="150"`)
		httpReq.Header.Set("sec-ch-ua-mobile", "?0")
		httpReq.Header.Set("sec-ch-ua-platform", `"Linux"`)
		httpReq.Header.Set("sec-ch-ua-bitness", `"64"`)
		httpReq.Header.Set("sec-ch-ua-arch", `"x86"`)
		if cookieHdr != "" {
			httpReq.Header.Set("Cookie", cookieHdr)
		}

		httpResp, err := plainClient.Do(httpReq)
		httpDuration := time.Since(httpStart)
		if err != nil {
			c.log.Warn("Generate request failed, will retry",
				zap.Error(err),
				zap.Duration("http_duration", httpDuration),
				zap.Int("attempt", attempt),
			)
			lastErr = err
			continue
		}
		defer httpResp.Body.Close()

		if httpResp.StatusCode != http.StatusOK {
			bodySnippet, _ := io.ReadAll(io.LimitReader(httpResp.Body, 512))
			lastErr = fmt.Errorf("generate failed with status: %d", httpResp.StatusCode)
			c.log.Warn("Generate returned non-200",
				zap.Int("status", httpResp.StatusCode),
				zap.String("body_snippet", string(bodySnippet)),
				zap.Int("attempt", attempt),
			)
			if httpResp.StatusCode >= 500 {
				continue
			}
			return nil, lastErr
		}

		respBytes, err := io.ReadAll(httpResp.Body)
		if err != nil {
			lastErr = fmt.Errorf("failed to read generate response: %w", err)
			continue
		}
		respBody := string(respBytes)

		parseStart := time.Now()
		result, parseErr := c.parseResponse(respBody)
		parseDuration := time.Since(parseStart)

		if parseErr != nil {
			lastErr = parseErr
			c.log.Warn("Failed to parse response, will retry",
				zap.Error(parseErr),
				zap.Int("attempt", attempt),
			)
			continue
		}

		c.log.Debug("GenerateContent timing",
			zap.Duration("gemini_server_rtt", httpDuration),
			zap.Duration("parse_duration", parseDuration),
			zap.Duration("total_duration", time.Since(totalStart)),
			zap.Int("attempt", attempt),
			zap.Int("response_bytes", len(respBody)),
		)

		if attempt > 1 {
			c.log.Info("GenerateContent succeeded after retry", zap.Int("attempt", attempt))
		}
		for i := range result.Images {
			if !result.Images[i].Generated {
				continue
			}
			encoded, downloadErr := c.downloadGeneratedImage(ctx, result.Images[i].URL, cookieHdr)
			if downloadErr != nil {
				c.log.Warn("Failed to download generated image", zap.Error(downloadErr))
				continue
			}
			result.Images[i].B64JSON = encoded
		}
		return result, nil
	}

	c.log.Error("GenerateContent failed after all attempts",
		zap.Int("attempts", maxAttempts),
		zap.Error(lastErr),
	)
	return nil, fmt.Errorf("after %d attempts: %w", maxAttempts, lastErr)
}

const maxGeneratedImageBytes = 50 << 20

func generatedImageHTTPClient(cookieHeader string) *http.Client {
	return &http.Client{
		Timeout: 2 * time.Minute,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) >= 10 {
				return fmt.Errorf("too many generated-image redirects")
			}
			if req.URL.Scheme != "https" || !isTrustedGoogleMediaHost(req.URL.Hostname()) {
				return fmt.Errorf("refusing generated-image redirect to untrusted host")
			}
			if cookieHeader != "" {
				req.Header.Set("Cookie", cookieHeader)
			}
			req.Header.Set("Referer", "https://gemini.google.com/")
			return nil
		},
	}
}

func (c *Client) downloadGeneratedImage(ctx context.Context, rawURL, cookieHeader string) (string, error) {
	imageURL := appendGoogleImageSize(rawURL, 2048)
	parsedImageURL, err := url.Parse(imageURL)
	if err != nil || parsedImageURL.Scheme != "https" || !isTrustedGoogleMediaHost(parsedImageURL.Hostname()) {
		return "", fmt.Errorf("refusing generated-image download from untrusted host")
	}

	cdpURL := os.Getenv("GEMINI_CDP_URL")
	if cdpURL == "" {
		cdpURL = "http://127.0.0.1:9225"
	}
	if b64, cdpErr := downloadGeneratedImageViaCDP(ctx, cdpURL, rawURL); cdpErr == nil && b64 != "" {
		return b64, nil
	} else if cdpErr != nil && c.log != nil {
		c.log.Debug("CDP image download failed, falling back to direct HTTP", zap.Error(cdpErr))
	}

	client := generatedImageHTTPClient(cookieHeader)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, imageURL, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36")
	req.Header.Set("Referer", "https://gemini.google.com/")
	if cookieHeader != "" {
		req.Header.Set("Cookie", cookieHeader)
	}
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK || !strings.HasPrefix(resp.Header.Get("Content-Type"), "image/") {
		return "", fmt.Errorf("generated image download returned %d %s", resp.StatusCode, resp.Header.Get("Content-Type"))
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxGeneratedImageBytes+1))
	if err != nil {
		return "", err
	}
	if len(body) > maxGeneratedImageBytes {
		return "", fmt.Errorf("generated image exceeds %d byte limit", maxGeneratedImageBytes)
	}
	return base64.StdEncoding.EncodeToString(body), nil
}

type cdpConn struct {
	conn net.Conn
	br   *bufio.Reader
}

func newCDPConn(rawWSURL string) (*cdpConn, error) {
	u, err := url.Parse(rawWSURL)
	if err != nil {
		return nil, err
	}
	host := u.Host
	if !strings.Contains(host, ":") {
		host += ":80"
	}
	conn, err := net.DialTimeout("tcp", host, 5*time.Second)
	if err != nil {
		return nil, err
	}

	nonce := make([]byte, 16)
	crand.Read(nonce)
	secKey := base64.StdEncoding.EncodeToString(nonce)

	req := fmt.Sprintf("GET %s HTTP/1.1\r\nHost: %s\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: %s\r\nSec-WebSocket-Version: 13\r\n\r\n",
		u.RequestURI(), u.Host, secKey)
	if _, err := conn.Write([]byte(req)); err != nil {
		conn.Close()
		return nil, err
	}

	br := bufio.NewReader(conn)
	resp, err := http.ReadResponse(br, &http.Request{Method: "GET"})
	if err != nil {
		conn.Close()
		return nil, err
	}
	if resp.StatusCode != http.StatusSwitchingProtocols {
		conn.Close()
		return nil, fmt.Errorf("websocket handshake returned status %d", resp.StatusCode)
	}

	return &cdpConn{conn: conn, br: br}, nil
}

func (c *cdpConn) send(payload []byte) error {
	var buf bytes.Buffer
	buf.WriteByte(0x81) // FIN + text opcode
	l := len(payload)
	mask := []byte{0x12, 0x34, 0x56, 0x78}
	if l < 126 {
		buf.WriteByte(byte(0x80 | l))
	} else if l <= 65535 {
		buf.WriteByte(byte(0x80 | 126))
		binary.Write(&buf, binary.BigEndian, uint16(l))
	} else {
		buf.WriteByte(byte(0x80 | 127))
		binary.Write(&buf, binary.BigEndian, uint64(l))
	}
	buf.Write(mask)

	masked := make([]byte, l)
	for i := 0; i < l; i++ {
		masked[i] = payload[i] ^ mask[i%4]
	}
	buf.Write(masked)

	_, err := c.conn.Write(buf.Bytes())
	return err
}

func (c *cdpConn) recv() ([]byte, error) {
	for {
		b0, err := c.br.ReadByte()
		if err != nil {
			return nil, err
		}
		b1, err := c.br.ReadByte()
		if err != nil {
			return nil, err
		}
		opcode := b0 & 0x0f
		isMasked := (b1 & 0x80) != 0
		length := int(b1 & 0x7f)

		if length == 126 {
			var l uint16
			binary.Read(c.br, binary.BigEndian, &l)
			length = int(l)
		} else if length == 127 {
			var l uint64
			binary.Read(c.br, binary.BigEndian, &l)
			length = int(l)
		}

		var mask [4]byte
		if isMasked {
			io.ReadFull(c.br, mask[:])
		}

		payload := make([]byte, length)
		if _, err := io.ReadFull(c.br, payload); err != nil {
			return nil, err
		}

		if isMasked {
			for i := 0; i < length; i++ {
				payload[i] ^= mask[i%4]
			}
		}

		if opcode == 0x01 { // text frame
			return payload, nil
		}
		if opcode == 0x08 { // close frame
			return nil, io.EOF
		}
	}
}

func (c *cdpConn) call(id int, method string, params map[string]interface{}) (map[string]interface{}, error) {
	reqBody := map[string]interface{}{
		"id":     id,
		"method": method,
		"params": params,
	}
	data, _ := json.Marshal(reqBody)
	if err := c.send(data); err != nil {
		return nil, err
	}
	for {
		respData, err := c.recv()
		if err != nil {
			return nil, err
		}
		var msg map[string]interface{}
		if err := json.Unmarshal(respData, &msg); err != nil {
			continue
		}
		if msgID, ok := msg["id"].(float64); ok && int(msgID) == id {
			if errObj, ok := msg["error"].(map[string]interface{}); ok {
				return nil, fmt.Errorf("cdp error: %v", errObj["message"])
			}
			if res, ok := msg["result"].(map[string]interface{}); ok {
				return res, nil
			}
			return msg, nil
		}
	}
}

func downloadGeneratedImageViaCDP(ctx context.Context, cdpBaseURL, rawURL string) (string, error) {
	httpReq, err := http.NewRequestWithContext(ctx, "GET", strings.TrimSuffix(cdpBaseURL, "/")+"/json/list", nil)
	if err != nil {
		return "", err
	}
	httpResp, err := http.DefaultClient.Do(httpReq)
	if err != nil {
		return "", err
	}
	defer httpResp.Body.Close()

	var targets []map[string]interface{}
	if err := json.NewDecoder(httpResp.Body).Decode(&targets); err != nil {
		return "", err
	}

	var target map[string]interface{}
	for _, t := range targets {
		if u, ok := t["url"].(string); ok && strings.Contains(u, "gemini.google.com") {
			target = t
			break
		}
	}
	if target == nil {
		return "", fmt.Errorf("no gemini page found in cdp")
	}

	wsURL, ok := target["webSocketDebuggerUrl"].(string)
	if !ok || wsURL == "" {
		return "", fmt.Errorf("no websocket debugger url in cdp target")
	}
	frameID, ok := target["id"].(string)
	if !ok || frameID == "" {
		return "", fmt.Errorf("no frame id in cdp target")
	}

	client, err := newCDPConn(wsURL)
	if err != nil {
		return "", err
	}
	defer client.conn.Close()

	currentURL := rawURL
	if !strings.Contains(currentURL, "=s") {
		currentURL = strings.TrimSuffix(currentURL, "/") + "=s1024-rj?authuser=" + AuthUser + "&alr=yes"
	}

	for step := 1; step <= 4; step++ {
		res, err := client.call(step, "Network.loadNetworkResource", map[string]interface{}{
			"frameId": frameID,
			"url":     currentURL,
			"options": map[string]interface{}{
				"disableCache":       true,
				"includeCredentials": true,
			},
		})
		if err != nil {
			return "", err
		}
		resource, ok := res["resource"].(map[string]interface{})
		if !ok {
			return "", fmt.Errorf("invalid resource in cdp response")
		}
		streamHandle, ok := resource["stream"].(string)
		if !ok || streamHandle == "" {
			return "", fmt.Errorf("no stream handle in cdp resource")
		}

		readRes, err := client.call(step+100, "IO.read", map[string]interface{}{
			"handle": streamHandle,
		})
		if err != nil {
			return "", err
		}
		data, ok := readRes["data"].(string)
		if !ok {
			return "", fmt.Errorf("invalid data in cdp io.read")
		}

		headers, _ := resource["headers"].(map[string]interface{})
		contentType, _ := headers["content-type"].(string)
		if strings.HasPrefix(contentType, "image/") {
			if isB64, _ := readRes["base64Encoded"].(bool); isB64 {
				return data, nil
			}
			return base64.StdEncoding.EncodeToString([]byte(data)), nil
		}
		currentURL = strings.TrimSpace(data)
	}
	return "", fmt.Errorf("cdp image download exceeded redirect hops")
}

// webModelIDs maps catalog model IDs onto internal Gemini Web model hashes
// (verified against boq_assistant-bard-web-server_20260910.05_p2 captures).
var webModelIDs = map[string]struct {
	id       string
	selector int
}{
	"gemini-3.8-flash":                 {id: "56fdd199312815e2", selector: 1},
	"gemini-3-flash":                   {id: "fbb127bbb056c959", selector: 1},
	"gemini-3-flash-thinking":          {id: "5bf011840784117a", selector: 2},
	"gemini-3.1-pro":                   {id: "e6fa609c3fa255c0", selector: 3},
	"gemini-3-pro":                     {id: "9d8ca3786ebdfbea", selector: 3},
	"gemini-3.5-flash-lite":            {id: "8c46e95b1a07cecc", selector: 6},
	"gemini-3.8-flash-plus":            {id: "56fdd199312815e2", selector: 1},
	"gemini-3.8-flash-advanced":        {id: "56fdd199312815e2", selector: 1},
	"gemini-3-flash-thinking-plus":     {id: "e051ce1aa80aa576", selector: 2},
	"gemini-3-pro-plus":                {id: "e6fa609c3fa255c0", selector: 3},
	"gemini-3-pro-advanced":            {id: "e6fa609c3fa255c0", selector: 3},
	"gemini-3-flash-thinking-advanced": {id: "e051ce1aa80aa576", selector: 2},
	"gemini-3.1-flash-lite":            {id: "8c46e95b1a07cecc", selector: 6},
	"gemini-3.5-flash":                 {id: "56fdd199312815e2", selector: 1},
	"gemini-3-flash-plus":              {id: "56fdd199312815e2", selector: 1},
	"gemini-3-flash-advanced":          {id: "56fdd199312815e2", selector: 1},
	"gemini-advanced":                  {id: "e6fa609c3fa255c0", selector: 3},
	"gemini-3-flash-preview":           {id: "fbb127bbb056c959", selector: 1},
	"gemini-4-argon":                   {id: "95a4c0abcba24364", selector: 8},
	"gemini-argon":                     {id: "95a4c0abcba24364", selector: 8},
	"gemini-4":                         {id: "95a4c0abcba24364", selector: 8},
}

// modelSelectorFor resolves the internal model hash and tier selector used in
// the x-goog-ext-525001261-jspb header. When no mapping exists the header keeps
// a random trace ID and the account default model answers.
func modelSelectorFor(model string) (string, int) {
	if entry, ok := webModelIDs[model]; ok {
		return entry.id, entry.selector
	}
	if strings.HasPrefix(model, "gemini-selector-") {
		var sel int
		if _, err := fmt.Sscanf(model, "gemini-selector-%d", &sel); err == nil {
			return "95a4c0abcba24364", sel
		}
	}
	return strings.ReplaceAll(uuid.NewString(), "-", "")[:16], 1
}

func resolveAvailableModel(requested string, models []ModelInfo) (string, bool) {
	requested = strings.TrimSpace(requested)
	if requested == "" {
		return "", false
	}

	for _, model := range models {
		if model.ID == requested {
			return model.ID, true
		}
	}

	var matches []string
	prefix := requested + "-"
	for _, model := range models {
		if strings.HasPrefix(model.ID, prefix) {
			matches = append(matches, model.ID)
		}
	}
	if len(matches) == 1 {
		return matches[0], true
	}

	return requested, false
}

func buildGenerateInner(prompt string, files []uploadedFile, model, language, requestID string, isTemporary bool, metadata *SessionMetadata) []interface{} {
	var messageContent []interface{}
	if len(files) == 0 {
		messageContent = []interface{}{prompt}
	} else {
		fileData := make([]interface{}, 0, len(files))
		for _, file := range files {
			fileData = append(fileData, []interface{}{
				[]interface{}{file.ID, 1, nil, "image/jpeg", uuid.NewString()},
				file.Name,
			})
		}
		messageContent = []interface{}{prompt, 0, nil, fileData, nil, nil, 0}
	}

	defaultMetadata := []interface{}{"", "", "", nil, nil, nil, nil, nil, nil, ""}
	if metadata != nil {
		defaultMetadata[0] = metadata.ConversationID
		defaultMetadata[1] = metadata.ResponseID
		defaultMetadata[2] = metadata.ChoiceID
		if metadata.Extra != nil {
			if contextToken, ok := metadata.Extra["context"].(string); ok {
				defaultMetadata[9] = contextToken
			}
		}
	}
	inner := make([]interface{}, 99)
	inner[0] = messageContent
	inner[1] = []interface{}{language}
	inner[2] = defaultMetadata
	inner[3] = model
	inner[6] = []interface{}{1}
	inner[7] = 1
	inner[10] = 1
	inner[11] = 0
	inner[17] = []interface{}{[]interface{}{0}}
	if metadata != nil && metadata.ResponseID != "" {
		inner[17] = []interface{}{[]interface{}{1}}
	}
	inner[18] = 0
	inner[27] = 1
	inner[30] = []interface{}{4}
	inner[41] = []interface{}{1}
	inner[49] = 0
	inner[53] = 0
	inner[59] = requestID
	inner[61] = []interface{}{}
	if metadata != nil && metadata.ConversationID != "" {
		inner[67] = 0
	}
	inner[68] = 2

	if len(files) > 0 {
		inner[17] = []interface{}{[]interface{}{2}}
		inner[30] = []interface{}{4, 16, 19}
		inner[79] = 8
		inner[80] = 1
		inner[91] = 1
		inner[96] = 0
		inner[98] = 1
	}

	if isTemporary {
		inner[45] = 1
		inner[67] = 0
	}

	return inner
}

func (c *Client) StartChat(options ...ChatOption) ChatSession {
	config := &ChatConfig{}
	for _, opt := range options {
		opt(config)
	}

	c.mu.RLock()
	if config.Model == "" || config.Model == "gemini-pro" {
		if len(c.cachedModels) > 0 {
			config.Model = c.cachedModels[0].ID
		}
	}
	c.mu.RUnlock()

	return &GeminiChatSession{
		client:   c,
		model:    config.Model,
		metadata: config.Metadata,
		history:  []Message{},
	}
}

func (c *Client) Close() error {
	close(c.stopRefresh)
	c.mu.Lock()
	c.healthy = false
	c.mu.Unlock()
	return nil
}

func (c *Client) GetName() string {
	return "gemini"
}

func (c *Client) IsHealthy() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.healthy
}

func (c *Client) ListModels() []ModelInfo {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if len(c.cachedModels) == 0 {
		return []ModelInfo{}
	}

	return c.cachedModels
}

func (c *Client) ListModelsIDs() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	ids := make([]string, 0, len(c.cachedModels))
	for _, m := range c.cachedModels {
		ids = append(ids, m.ID)
	}
	return ids
}

// parseResponse parses Gemini's response format
func (c *Client) parseResponse(text string) (*Response, error) {
	var finalResText string
	var finalMetadata map[string]any
	found := false
	imagesByURL := make(map[string]Image)

	lines := strings.Split(text, "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		line = strings.TrimPrefix(line, ")]}'")

		var root []interface{}
		if err := json.Unmarshal([]byte(line), &root); err == nil {
			for _, item := range root {
				itemArray, ok := item.([]interface{})
				if !ok || len(itemArray) < 1 {
					continue
				}

				// Check for BardErrorInfo error block
				if errStr := extractBardError(itemArray); errStr != "" {
					return nil, errors.New(errStr)
				}

				if len(itemArray) < 3 {
					continue
				}

				payloadStr, ok := itemArray[2].(string)
				if !ok {
					continue
				}

				var payload []interface{}
				if err := json.Unmarshal([]byte(payloadStr), &payload); err != nil {
					continue
				}
				collectImages(payload, imagesByURL)
				if len(payload) > 4 {
					candidates, ok := payload[4].([]interface{})
					if ok && candidates != nil && len(candidates) > 0 {
						for _, rawCandidate := range candidates {
							candidate, ok := rawCandidate.([]interface{})
							if !ok {
								continue
							}
							for _, image := range extractGeneratedImages(candidate) {
								imagesByURL[image.URL] = image
							}
						}
						firstCandidate, ok := candidates[0].([]interface{})
						if ok && len(firstCandidate) >= 2 {
							contentParts, ok := firstCandidate[1].([]interface{})
							if ok && len(contentParts) > 0 {
								resText, ok := contentParts[0].(string)
								if ok {
									// Extract conversation metadata if available.
									var cid, rid, rcid, contextToken string
									if len(firstCandidate) > 0 {
										if id, ok := firstCandidate[0].(string); ok {
											rcid = id
										}
									}
									if len(payload) > 1 {
										if metadataArray, ok := payload[1].([]interface{}); ok {
											if len(metadataArray) > 0 {
												cid, _ = metadataArray[0].(string)
											}
											if len(metadataArray) > 1 {
												rid, _ = metadataArray[1].(string)
											}
											if len(metadataArray) > 2 && rcid == "" {
												rcid, _ = metadataArray[2].(string)
											}
											if len(metadataArray) > 9 {
												contextToken, _ = metadataArray[9].(string)
											}
										} else {
											cid, _ = payload[1].(string)
										}
									}

									finalResText = resText
									finalMetadata = map[string]any{
										"cid":     cid,
										"rid":     rid,
										"rcid":    rcid,
										"context": contextToken,
									}
									found = true
								}
							}
						}
					}
				}
			}
		}
	}

	if found || len(imagesByURL) > 0 {
		images := make([]Image, 0, len(imagesByURL))
		for _, image := range imagesByURL {
			images = append(images, image)
		}
		reasoning, cleanText := utils.ExtractThinkingAndText(finalResText)
		return &Response{
			Text:          cleanText,
			ReasoningText: reasoning,
			Images:        images,
			Metadata:      finalMetadata,
		}, nil
	}

	sample := text
	if len(sample) > 500 {
		sample = sample[:500]
	}
	return nil, fmt.Errorf("failed to parse response. Sample: %s", sample)
}

// extractBardError recursively searches for BardErrorInfo and extracts codes/messages
func extractBardError(item []interface{}) string {
	var codes []int
	var foundError bool

	var tempCodes []int
	var walk func(v any) bool
	walk = func(v any) bool {
		switch val := v.(type) {
		case []interface{}:
			hasError := false
			for _, child := range val {
				if walk(child) {
					hasError = true
				}
			}
			return hasError
		case string:
			return strings.Contains(val, "BardErrorInfo")
		case float64:
			tempCodes = append(tempCodes, int(val))
			return false
		}
		return false
	}

	for _, el := range item {
		tempCodes = nil
		if walk(el) {
			foundError = true
			codes = tempCodes
			break
		}
	}

	if foundError {
		if len(codes) > 0 {
			return fmt.Sprintf("Google Gemini Web returned an error (BardErrorInfo code %v). This usually indicates session expiration, rate limits, context window limits, or bot protection/CAPTCHA block.", codes)
		}
		return "Google Gemini Web returned a BardErrorInfo block."
	}
	return ""
}

func collectImages(value any, out map[string]Image) {
	switch v := value.(type) {
	case []interface{}:
		for _, item := range v {
			collectImages(item, out)
		}
	case map[string]interface{}:
		for _, item := range v {
			collectImages(item, out)
		}
	case string:
		for _, rawURL := range imageURLRegex.FindAllString(v, -1) {
			imageURL := normalizeImageURL(rawURL)
			if imageURL == "" {
				continue
			}
			if _, exists := out[imageURL]; exists {
				continue
			}
			out[imageURL] = Image{
				URL:      imageURL,
				MimeType: mimeTypeFromImageURL(imageURL),
			}
		}
	}
}

// extractGeneratedImages reads the dedicated Gemini Web generated-media slot.
// Current responses store generated images at candidate[12][7][0], where each
// item contains metadata at item[0][3]: filename, URL, MIME and dimensions.
// Generic URL scanning misses some of these URLs because they often have no
// file extension and may be embedded beside non-image googleusercontent URLs.
func extractGeneratedImages(candidate []interface{}) []Image {
	if len(candidate) <= 12 {
		return nil
	}
	candidateMedia, ok := candidate[12].([]interface{})
	if !ok || len(candidateMedia) <= 7 {
		return nil
	}
	mediaGroups, ok := candidateMedia[7].([]interface{})
	if !ok || len(mediaGroups) == 0 {
		return nil
	}
	generated, ok := mediaGroups[0].([]interface{})
	if !ok {
		return nil
	}

	images := make([]Image, 0, len(generated))
	for _, rawImage := range generated {
		imageNode, ok := rawImage.([]interface{})
		if !ok || len(imageNode) == 0 {
			continue
		}
		wrapper, ok := imageNode[0].([]interface{})
		if !ok || len(wrapper) <= 3 {
			continue
		}
		metadata, ok := wrapper[3].([]interface{})
		if !ok || len(metadata) <= 3 {
			continue
		}
		imageURL, ok := metadata[3].(string)
		if !ok || normalizeImageURL(imageURL) == "" {
			continue
		}
		image := Image{URL: normalizeImageURL(imageURL), MimeType: "image/png", Generated: true}
		if filename, ok := metadata[2].(string); ok {
			image.Title = filename
		}
		for _, field := range metadata {
			switch typed := field.(type) {
			case string:
				if strings.HasPrefix(typed, "image/") {
					image.MimeType = typed
				}
			case []interface{}:
				if len(typed) >= 2 {
					if width, ok := typed[0].(float64); ok {
						image.Width = int(width)
					}
					if height, ok := typed[1].(float64); ok {
						image.Height = int(height)
					}
				}
			}
		}
		images = append(images, image)
	}
	return images
}

func appendGoogleImageSize(rawURL string, size int) string {
	if size <= 0 {
		return rawURL
	}
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return rawURL
	}
	lastSegment := path.Base(parsed.Path)
	if strings.Contains(lastSegment, "=s") || strings.Contains(lastSegment, "=w") {
		return rawURL
	}
	parsed.Path = strings.TrimSuffix(parsed.Path, "/") + fmt.Sprintf("=s%d", size)
	return parsed.String()
}

func isTrustedGoogleMediaHost(host string) bool {
	host = strings.ToLower(strings.TrimSuffix(strings.TrimSpace(host), "."))
	return host == "google.com" || strings.HasSuffix(host, ".google.com") ||
		host == "googleusercontent.com" || strings.HasSuffix(host, ".googleusercontent.com")
}

func normalizeImageURL(rawURL string) string {
	cleaned := html.UnescapeString(strings.TrimSpace(rawURL))
	cleaned = strings.TrimRight(cleaned, ".,);]")
	if strings.HasPrefix(cleaned, "//") {
		cleaned = "https:" + cleaned
	}
	lowerCleaned := strings.ToLower(cleaned)
	if strings.HasPrefix(lowerCleaned, "googleusercontent.com/") || strings.HasSuffix(strings.SplitN(lowerCleaned, "/", 2)[0], ".googleusercontent.com") {
		cleaned = "https://" + cleaned
	}
	parsed, err := url.Parse(cleaned)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return ""
	}
	if !looksLikeImageURL(parsed) {
		return ""
	}
	if strings.Contains(strings.ToLower(parsed.Hostname()), "googleusercontent.com") {
		parsed.Scheme = "https"
	}
	return parsed.String()
}

func looksLikeImageURL(u *url.URL) bool {
	host := strings.ToLower(u.Hostname())
	path := strings.ToLower(u.EscapedPath())

	if strings.HasSuffix(path, ".svg") || strings.Contains(host, "fonts.gstatic.com") {
		return false
	}
	if host == "googleusercontent.com" {
		return false
	}
	if strings.HasSuffix(host, ".googleusercontent.com") {
		return true
	}
	switch {
	case strings.HasSuffix(path, ".png"),
		strings.HasSuffix(path, ".jpg"),
		strings.HasSuffix(path, ".jpeg"),
		strings.HasSuffix(path, ".webp"),
		strings.HasSuffix(path, ".gif"):
		return true
	default:
		return false
	}
}

func mimeTypeFromImageURL(rawURL string) string {
	u, err := url.Parse(rawURL)
	if err != nil {
		return ""
	}
	path := strings.ToLower(u.EscapedPath())
	switch {
	case strings.HasSuffix(path, ".png"):
		return "image/png"
	case strings.HasSuffix(path, ".jpg"), strings.HasSuffix(path, ".jpeg"):
		return "image/jpeg"
	case strings.HasSuffix(path, ".webp"):
		return "image/webp"
	case strings.HasSuffix(path, ".gif"):
		return "image/gif"
	default:
		return ""
	}
}

func (cs *CookieStore) ToHTTPCookies() []*http.Cookie {
	cs.mu.RLock()
	defer cs.mu.RUnlock()

	cookies := []*http.Cookie{}
	domain := ".google.com"

	if cs.Secure1PSID != "" {
		cookies = append(cookies, &http.Cookie{
			Name:     "__Secure-1PSID",
			Value:    cleanCookie(cs.Secure1PSID),
			Domain:   domain,
			Path:     "/",
			Secure:   true,
			HttpOnly: true,
			SameSite: http.SameSiteNoneMode,
		})
	}
	if cs.Secure1PSIDTS != "" {
		cookies = append(cookies, &http.Cookie{
			Name:     "__Secure-1PSIDTS",
			Value:    cleanCookie(cs.Secure1PSIDTS),
			Domain:   domain,
			Path:     "/",
			Secure:   true,
			HttpOnly: true,
			SameSite: http.SameSiteNoneMode,
		})
	}
	return cookies
}

func cleanCookie(v string) string {
	v = strings.TrimSpace(v)
	v = strings.Trim(v, "\"")
	v = strings.Trim(v, "'")
	v = strings.TrimSuffix(v, ";")
	return v
}

// LoadCachedCookies attempts to read the saved 1PSIDTS from disk
func (c *Client) LoadCachedCookies() (string, error) {
	if c.cookies.Secure1PSID == "" {
		return "", errors.New("no PSID available")
	}

	hash := sha256.Sum256([]byte(c.cookies.Secure1PSID))
	filename := filepath.Join(".cookies", hex.EncodeToString(hash[:])+".txt")

	data, err := os.ReadFile(filename)
	if err != nil {
		return "", err
	}

	ts := strings.TrimSpace(string(data))
	if ts == "" {
		return "", errors.New("empty cache file")
	}
	return ts, nil
}

// SaveCachedCookies writes the current 1PSIDTS to disk
func (c *Client) SaveCachedCookies() error {
	if c.cookies.Secure1PSID == "" || c.cookies.Secure1PSIDTS == "" {
		return nil
	}

	// Create directory if not exists
	if err := os.MkdirAll(".cookies", 0755); err != nil {
		return err
	}

	hash := sha256.Sum256([]byte(c.cookies.Secure1PSID))
	filename := filepath.Join(".cookies", hex.EncodeToString(hash[:])+".txt")

	err := os.WriteFile(filename, []byte(c.cookies.Secure1PSIDTS), 0600)
	if err == nil {
		c.log.Debug("Saved __Secure-1PSIDTS to local cache for future use", zap.String("file", filename))
	} else {
		c.log.Warn("Failed to save cookies to cache", zap.String("file", filename), zap.Error(err))
	}
	return err
}

// ClearCookieCache deletes the cached cookie file for the current PSID
func (c *Client) ClearCookieCache() error {
	if c.cookies.Secure1PSID == "" {
		return nil
	}

	hash := sha256.Sum256([]byte(c.cookies.Secure1PSID))
	filename := filepath.Join(".cookies", hex.EncodeToString(hash[:])+".txt")

	err := os.Remove(filename)
	if err != nil && !os.IsNotExist(err) {
		return err
	}

	return nil
}

var (
	AuthUser = func() string {
		if u := os.Getenv("GEMINI_AUTH_USER"); u != "" {
			return u
		}
		return "1"
	}()

	EndpointGoogle        = "https://www.google.com"
	EndpointInit          = "https://gemini.google.com/u/" + AuthUser + "/app"
	EndpointGenerate      = "https://gemini.google.com/u/" + AuthUser + "/_/BardChatUi/data/assistant.lamda.BardFrontendService/StreamGenerate"
	EndpointRotateCookies = "https://accounts.google.com/RotateCookies"
	EndpointUpload        = "https://content-push.googleapis.com/upload"
	EndpointBatchExec     = "https://gemini.google.com/u/" + AuthUser + "/_/BardChatUi/data/batchexecute"
)

var DefaultHeaders = map[string]string{
	"Content-Type":    "application/x-www-form-urlencoded;charset=utf-8",
	"Origin":          "https://gemini.google.com",
	"Referer":         "https://gemini.google.com/",
	"User-Agent":      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
	"X-Same-Domain":   "1",
	"X-Goog-AuthUser": AuthUser,
}
