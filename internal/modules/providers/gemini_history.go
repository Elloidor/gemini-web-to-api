package providers

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"html"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
)

const (
	listChatsRPCID = "MaZiqc"
	readChatRPCID  = "hNvQHb"
)

// ChatListItem is one Gemini Web conversation returned by the browser history RPC.
type ChatListItem struct {
	ConversationID string `json:"chat_id"`
	Title          string `json:"title"`
	UpdatedAt      int64  `json:"updated_at"`
}

// ChatTurn is one user/model exchange from a Gemini Web conversation.
type ChatTurn struct {
	RequestID   string `json:"request_id"`
	CandidateID string `json:"candidate_id"`
	UserText    string `json:"user_text"`
	ModelText   string `json:"model_text"`
	CreatedAt   int64  `json:"created_at"`
}

// ChatPage contains one page of browser conversations.
type ChatPage struct {
	Chats      []ChatListItem `json:"chats"`
	NextCursor string         `json:"next_cursor,omitempty"`
}

// ChatHistoryPage contains one page of turns from a browser conversation.
type ChatHistoryPage struct {
	ConversationID string     `json:"chat_id"`
	Turns          []ChatTurn `json:"turns"`
	NextCursor     string     `json:"next_cursor,omitempty"`
}

// ListChats reads Gemini Web browser history. Cursor is the opaque value from a previous page.
func (c *Client) ListChats(ctx context.Context, pageSize int, cursor string) (*ChatPage, error) {
	if pageSize <= 0 {
		pageSize = 13
	}
	if pageSize > 100 {
		pageSize = 100
	}

	variants := [][2]int{{1, 1}, {0, 1}, {0, 2}}
	if cursor != "" {
		variants = [][2]int{{0, 1}}
	}

	var lastErr error
	for _, flags := range variants {
		var cursorValue any
		if cursor != "" {
			cursorValue = cursor
		}
		payload, err := json.Marshal([]any{pageSize, cursorValue, []any{flags[0], nil, flags[1]}})
		if err != nil {
			return nil, err
		}
		body, rejectCode, err := c.callBatchRPC(ctx, listChatsRPCID, string(payload), "/app")
		if err != nil {
			lastErr = err
			continue
		}
		if rejectCode != 0 {
			lastErr = fmt.Errorf("list chats rejected with code %d", rejectCode)
			continue
		}
		page, err := decodeChatList(body)
		if err != nil {
			lastErr = err
			continue
		}
		if len(page.Chats) > 0 || page.NextCursor != "" {
			return page, nil
		}
	}
	if lastErr != nil {
		return nil, lastErr
	}
	return &ChatPage{Chats: []ChatListItem{}}, nil
}

// ReadChat reads browser-visible turns from a Gemini Web conversation.
func (c *Client) ReadChat(ctx context.Context, conversationID string, maxTurns int, cursor string) (*ChatHistoryPage, error) {
	conversationID = strings.TrimSpace(conversationID)
	if !strings.HasPrefix(conversationID, "c_") {
		return nil, fmt.Errorf("invalid chat id %q", conversationID)
	}
	if maxTurns <= 0 {
		maxTurns = 10
	}
	if maxTurns > 100 {
		maxTurns = 100
	}

	var cursorValue any
	if cursor != "" {
		cursorValue = cursor
	}
	payload, err := json.Marshal([]any{conversationID, maxTurns, cursorValue, 1, []any{0}, []any{4}, nil, 1})
	if err != nil {
		return nil, err
	}
	body, rejectCode, err := c.callBatchRPC(ctx, readChatRPCID, string(payload), "/app/"+conversationID)
	if err != nil {
		return nil, err
	}
	if rejectCode != 0 {
		return nil, fmt.Errorf("read chat rejected with code %d", rejectCode)
	}
	page, err := decodeChatHistory(body)
	if err != nil {
		return nil, err
	}
	page.ConversationID = conversationID
	return page, nil
}

// ContinueChat resolves the latest rid/rcid itself and appends a message to an existing browser chat.
func (c *Client) ContinueChat(ctx context.Context, conversationID, prompt string, options ...GenerateOption) (*Response, error) {
	lockValue, _ := c.chatLocks.LoadOrStore(conversationID, &sync.Mutex{})
	chatLock := lockValue.(*sync.Mutex)
	chatLock.Lock()
	defer chatLock.Unlock()

	if strings.TrimSpace(prompt) == "" {
		return nil, fmt.Errorf("prompt is required")
	}
	history, err := c.ReadChat(ctx, conversationID, 10, "")
	if err != nil {
		return nil, err
	}

	var latest *ChatTurn
	for i := len(history.Turns) - 1; i >= 0; i-- {
		turn := &history.Turns[i]
		if turn.RequestID != "" && turn.CandidateID != "" {
			latest = turn
			break
		}
	}
	if latest == nil {
		return nil, fmt.Errorf("chat %s has no completed model response to continue", conversationID)
	}

	metadata := &SessionMetadata{
		ConversationID: conversationID,
		ResponseID:     latest.RequestID,
		ChoiceID:       latest.CandidateID,
	}
	return c.generateContent(ctx, prompt, metadata, options...)
}

func (c *Client) callBatchRPC(ctx context.Context, rpcID, payload, sourcePath string) ([]byte, int, error) {
	if err := c.ensureCurrentBrowserSession(); err != nil {
		return nil, 0, fmt.Errorf("refresh browser session: %w", err)
	}
	c.mu.RLock()
	at := c.at
	cookieHeader := c.cookieHeader
	buildLabel := c.buildLabel
	sessionID := c.sessionID
	language := c.language
	c.mu.RUnlock()
	if at == "" {
		return nil, 0, fmt.Errorf("client not initialized")
	}
	if language == "" {
		language = "en"
	}

	requestEnvelope, err := json.Marshal([]any{[]any{[]any{rpcID, payload, nil, "generic"}}})
	if err != nil {
		return nil, 0, err
	}
	form := url.Values{}
	form.Set("at", at)
	form.Set("f.req", string(requestEnvelope))

	query := url.Values{}
	query.Set("rpcids", rpcID)
	query.Set("_reqid", strconv.FormatInt(time.Now().UnixNano()%90000+10000, 10))
	query.Set("rt", "c")
	query.Set("hl", language)
	query.Set("pageId", "none")
	query.Set("source-path", sourcePath)
	if buildLabel != "" {
		query.Set("bl", buildLabel)
	}
	if sessionID != "" {
		query.Set("f.sid", sessionID)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, EndpointBatchExec+"?"+query.Encode(), strings.NewReader(form.Encode()))
	if err != nil {
		return nil, 0, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded;charset=utf-8")
	req.Header.Set("Origin", "https://gemini.google.com")
	req.Header.Set("Referer", "https://gemini.google.com"+sourcePath)
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36")
	req.Header.Set("X-Same-Domain", "1")
	if cookieHeader != "" {
		req.Header.Set("Cookie", cookieHeader)
	}

	resp, err := (&http.Client{Timeout: 2 * time.Minute}).Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, 0, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, 0, fmt.Errorf("batchexecute returned HTTP %d: %s", resp.StatusCode, responseSnippet(body, 300))
	}
	return extractBatchRPCBody(stripBatchPrefix(body), rpcID)
}

func decodeChatList(body []byte) (*ChatPage, error) {
	var data []any
	if len(bytes.TrimSpace(body)) == 0 {
		return &ChatPage{Chats: []ChatListItem{}}, nil
	}
	if err := json.Unmarshal(body, &data); err != nil {
		return nil, fmt.Errorf("decode chat list: %w", err)
	}
	page := &ChatPage{Chats: []ChatListItem{}, NextCursor: nestedString(data, 1)}
	items, ok := nestedArray(data, 2)
	if !ok {
		return page, nil
	}
	for _, raw := range items {
		item, ok := raw.([]any)
		if !ok {
			continue
		}
		cid := nestedString(item, 0)
		if cid == "" {
			continue
		}
		page.Chats = append(page.Chats, ChatListItem{
			ConversationID: cid,
			Title:          html.UnescapeString(nestedString(item, 1)),
			UpdatedAt:      nestedInt64(item, 5, 0),
		})
	}
	return page, nil
}

func decodeChatHistory(body []byte) (*ChatHistoryPage, error) {
	var data []any
	if len(bytes.TrimSpace(body)) == 0 {
		return &ChatHistoryPage{Turns: []ChatTurn{}}, nil
	}
	if err := json.Unmarshal(body, &data); err != nil {
		return nil, fmt.Errorf("decode chat history: %w", err)
	}
	page := &ChatHistoryPage{Turns: []ChatTurn{}, NextCursor: nestedString(data, 1)}
	items, ok := nestedArray(data, 0)
	if !ok {
		return page, nil
	}
	for _, raw := range items {
		turn, ok := raw.([]any)
		if !ok {
			continue
		}
		candidate, _ := nestedArray(turn, 3, 0, 0)
		decoded := ChatTurn{
			RequestID: nestedString(turn, 0, 1),
			UserText:  html.UnescapeString(nestedString(turn, 2, 0, 0)),
			CreatedAt: nestedInt64(turn, 4, 0),
		}
		if len(candidate) > 0 {
			decoded.CandidateID = nestedString(candidate, 0)
			decoded.ModelText = html.UnescapeString(nestedString(candidate, 1, 0))
			if decoded.ModelText == "" {
				decoded.ModelText = html.UnescapeString(nestedString(candidate, 22, 0))
			}
		}
		if decoded.UserText != "" || decoded.ModelText != "" {
			page.Turns = append(page.Turns, decoded)
		}
	}
	for i, j := 0, len(page.Turns)-1; i < j; i, j = i+1, j-1 {
		page.Turns[i], page.Turns[j] = page.Turns[j], page.Turns[i]
	}
	return page, nil
}

func stripBatchPrefix(body []byte) []byte {
	return bytes.TrimPrefix(body, []byte(")]}'\n"))
}

func extractBatchRPCBody(body []byte, rpcID string) ([]byte, int, error) {
	for _, frame := range parseLengthPrefixedFrames(body) {
		var root []any
		if json.Unmarshal(frame, &root) != nil {
			continue
		}
		for _, item := range findWrbItems(root) {
			if len(item) < 3 || nestedString(item, 0) != "wrb.fr" || nestedString(item, 1) != rpcID {
				continue
			}
			var payload []byte
			if value, ok := item[2].(string); ok {
				payload = []byte(value)
			}
			return payload, int(nestedInt64(item, 5, 0)), nil
		}
	}
	return nil, 0, fmt.Errorf("RPC response for %s not found", rpcID)
}

func parseLengthPrefixedFrames(content []byte) [][]byte {
	var frames [][]byte
	pos := 0
	for pos < len(content) {
		for pos < len(content) && strings.ContainsRune(" \t\r\n", rune(content[pos])) {
			pos++
		}
		start := pos
		for pos < len(content) && content[pos] >= '0' && content[pos] <= '9' {
			pos++
		}
		if start == pos || pos >= len(content) || content[pos] != '\n' {
			pos++
			continue
		}
		units, err := strconv.Atoi(string(content[start:pos]))
		if err != nil {
			continue
		}
		contentStart := pos
		consumed := 0
		for pos < len(content) && consumed < units {
			r, size := utf8.DecodeRune(content[pos:])
			if size == 0 {
				break
			}
			consumed++
			if r > 0xFFFF {
				consumed++
			}
			pos += size
		}
		if consumed < units {
			break
		}
		frame := bytes.TrimSpace(content[contentStart:pos])
		if len(frame) > 0 {
			frames = append(frames, frame)
		}
	}
	return frames
}

func findWrbItems(root []any) [][]any {
	var found [][]any
	var walk func(any)
	walk = func(value any) {
		array, ok := value.([]any)
		if !ok {
			return
		}
		if len(array) >= 2 && nestedString(array, 0) == "wrb.fr" {
			found = append(found, array)
			return
		}
		for _, child := range array {
			walk(child)
		}
	}
	walk(root)
	return found
}

func nestedValue(value any, indexes ...int) (any, bool) {
	current := value
	for _, index := range indexes {
		array, ok := current.([]any)
		if !ok || index < 0 || index >= len(array) {
			return nil, false
		}
		current = array[index]
	}
	return current, true
}

func nestedArray(value any, indexes ...int) ([]any, bool) {
	v, ok := nestedValue(value, indexes...)
	if !ok {
		return nil, false
	}
	array, ok := v.([]any)
	return array, ok
}

func nestedString(value any, indexes ...int) string {
	v, ok := nestedValue(value, indexes...)
	if !ok {
		return ""
	}
	text, _ := v.(string)
	return text
}

func nestedInt64(value any, indexes ...int) int64 {
	v, ok := nestedValue(value, indexes...)
	if !ok {
		return 0
	}
	switch n := v.(type) {
	case float64:
		return int64(n)
	case int64:
		return n
	case json.Number:
		result, _ := n.Int64()
		return result
	}
	return 0
}

func responseSnippet(body []byte, limit int) string {
	if len(body) <= limit {
		return string(body)
	}
	return string(body[:limit])
}
