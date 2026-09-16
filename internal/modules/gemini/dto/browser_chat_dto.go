package dto

// BrowserChatMessageRequest appends a user message to an existing gemini.google.com chat.
type BrowserChatMessageRequest struct {
	Prompt string `json:"prompt" binding:"required"`
	Model  string `json:"model,omitempty"`
}

// BrowserChatMessageResponse identifies the new turn written to the browser chat.
type BrowserChatMessageResponse struct {
	ChatID        string `json:"chat_id"`
	RequestID     string `json:"request_id,omitempty"`
	CandidateID   string `json:"candidate_id,omitempty"`
	Text          string `json:"text"`
	ReasoningText string `json:"reasoning_text,omitempty"`
}
