package api

import (
	"errors"
	"fmt"
	"net/mail"
	"strings"

	"github.com/7vignesh/mailq/internal/store"
)

// validate checks a request at the trust boundary. Addresses must be bare
// (no display names), because they go straight into MAIL FROM / RCPT TO;
// CR and LF are rejected everywhere they could reach a header or command.
func validate(r postRequest) (store.NewMessage, error) {
	var errs []error
	from, err := bareAddress("from", r.From)
	errs = append(errs, err)
	to, err := bareAddress("to", r.To)
	errs = append(errs, err)

	switch {
	case r.IdempotencyKey == "":
		errs = append(errs, errors.New("idempotency_key is required"))
	case len(r.IdempotencyKey) > 255:
		errs = append(errs, errors.New("idempotency_key must be at most 255 bytes"))
	}
	if strings.ContainsAny(r.Subject, "\r\n") {
		errs = append(errs, errors.New("subject must not contain line breaks"))
	}
	if len(r.Subject) > 998 {
		errs = append(errs, errors.New("subject must be at most 998 bytes"))
	}
	if err := errors.Join(errs...); err != nil {
		return store.NewMessage{}, err
	}
	_, domain, _ := strings.Cut(to, "@")
	return store.NewMessage{
		IdempotencyKey: r.IdempotencyKey, From: from, To: to, Domain: strings.ToLower(domain),
		Subject: r.Subject, Body: r.Body,
	}, nil
}

func bareAddress(field, s string) (string, error) {
	if s == "" {
		return "", fmt.Errorf("%s is required", field)
	}
	if len(s) > 254 || strings.ContainsAny(s, "\r\n<> ") {
		return "", fmt.Errorf("%s must be a bare address like user@example.com", field)
	}
	a, err := mail.ParseAddress(s)
	if err != nil || a.Name != "" || a.Address != s {
		return "", fmt.Errorf("%s must be a bare address like user@example.com", field)
	}
	if _, domain, _ := strings.Cut(s, "@"); !strings.Contains(domain, ".") && domain != "localhost" {
		return "", fmt.Errorf("%s domain %q is not a fully qualified name", field, domain)
	}
	return s, nil
}
