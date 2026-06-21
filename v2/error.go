package jibby

// ParseError records JSON/Extended JSON parsing errors.  It can include a small
// excerpt of text from the reader at the point of error.
type ParseError struct {
	msg string
}

func (pe *ParseError) Error() string { return pe.msg }
