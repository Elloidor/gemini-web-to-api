package gemini

import (
	"fmt"

	"gemini-web-to-api/internal/commons/configs"
	"gemini-web-to-api/internal/commons/utils"
	"gemini-web-to-api/internal/modules/providers"

	"github.com/gofiber/fiber/v3"
	"go.uber.org/fx"
)

var Module = fx.Options(
	fx.Provide(providers.NewClient),
	fx.Provide(NewGeminiService),
	fx.Provide(NewGeminiController),
	fx.Invoke(RegisterRoutes),
)

func RegisterRoutes(app *fiber.App, c *GeminiController, cfg *configs.Config) {
	// Official-compatible Gemini routes.
	geminiGroup := app.Group("/gemini")
	geminiV1 := geminiGroup.Group("/v1beta")
	c.Register(geminiV1)

	// Browser-history extension for chats created in gemini.google.com.
	// Disabled unless explicitly enabled; requires a matching API key on every request.
	if !cfg.Gemini.BrowserChatsEnabled {
		return
	}
	webGroup := geminiGroup.Group("/web", browserChatGuard(cfg))
	c.RegisterBrowserChats(webGroup)
}

func browserChatGuard(cfg *configs.Config) fiber.Handler {
	return func(c fiber.Ctx) error {
		if !cfg.BrowserChatAuthorized(c.Get("Authorization"), c.Query("key")) {
			return c.Status(fiber.StatusUnauthorized).JSON(
				utils.ErrorToResponse(fmt.Errorf("missing or invalid API key"), "invalid_api_key"))
		}
		return c.Next()
	}
}
