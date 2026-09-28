package api

import (
	"encoding/json"
	"errors"

	"github.com/AbePlays/go-worker-pool-system-design/utils"
)

func ValidateRequest(req CreateJobRequest) error {
	switch req.Type {
	case "sleep":
		if req.Payload.DurationMs < 0 || req.Payload.DurationMs > 30000 {
			return errors.New("invalid payload duration")
		}
	case "webhook":
		if !utils.ValidateUrl(req.Payload.Url) {
			return errors.New("invalid payload url")
		}
		if req.Payload.Body != "" && !json.Valid([]byte(req.Payload.Body)) {
			return errors.New("invalid payload body: must be valid JSON")
		}
	case "image":
		if !utils.ValidateUrl(req.Payload.ImageUrl) {
			return errors.New("invalid image url")
		}
	default:
		return errors.New("invalid job type")
	}
	return nil
}
