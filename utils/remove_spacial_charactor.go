package utils

import (
	"regexp"
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

func ReplaceSpacialCharactor(Oldstring string) (Newstring string, err error) {
	// Make a Regex to say we only want letters and numbers
	reg, err := regexp.Compile("[^a-zA-Z0-9]+")
	if err != nil {
		return Newstring, err
	}
	Newstring = reg.ReplaceAllString(Oldstring, "")
	return Newstring, nil
}
