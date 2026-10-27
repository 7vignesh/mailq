package smtp

import (
	"bytes"
	"fmt"
	"mime"
	"mime/quotedprintable"
	"strings"
	"time"
)

// BuildMessage renders a plain-text RFC 5322 message with CRLF line endings.
//
// The body is quoted-printable encoded, so the message is 7-bit clean (no
// need for the 8BITMIME extension) and no line exceeds 76 characters,
// well under SMTP's 998-character limit. Dot-stuffing is NOT done here; it
// belongs to the DATA transport (see writeBody).
//
// Message-ID is derived from the queue id, so every retry of a message
// carries the same Message-ID. Unverified: some receivers may use it to
// collapse duplicates, but we do not rely on that.
func BuildMessage(id, from, to, subject, body, domain string, date time.Time) []byte {
	var b bytes.Buffer
	header := func(k, v string) { fmt.Fprintf(&b, "%s: %s\r\n", k, oneLine(v)) }
	header("From", from)
	header("To", to)
	header("Subject", mime.QEncoding.Encode("utf-8", oneLine(subject)))
	header("Date", date.Format(time.RFC1123Z))
	header("Message-ID", "<"+id+"@"+domain+">")
	header("MIME-Version", "1.0")
	header("Content-Type", "text/plain; charset=utf-8")
	header("Content-Transfer-Encoding", "quoted-printable")
	b.WriteString("\r\n")

	qp := quotedprintable.NewWriter(&b)
	qp.Write([]byte(body))
	qp.Close()
	if !bytes.HasSuffix(b.Bytes(), []byte("\r\n")) {
		b.WriteString("\r\n")
	}
	return b.Bytes()
}

// oneLine strips CR and LF, so a header value can never inject another
// header. Input is validated at ingest too; this is defense in depth.
func oneLine(s string) string {
	return strings.NewReplacer("\r", "", "\n", "").Replace(s)
}
