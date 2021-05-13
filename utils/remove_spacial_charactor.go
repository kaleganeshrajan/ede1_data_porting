package utils

import (
	"strings"
	"unicode"
)

func Removespacialcharactor(charstring string) string {
	charstring = strings.Map(func(r rune) rune {
		if unicode.IsGraphic(r) {
			return r
		}
		return -1
	}, charstring)
	return charstring
}
