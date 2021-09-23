//! Typed implementation of a NATS subject.

use std::{fmt, str::FromStr};

/// Wildcard matching a single [`Token`].
pub const SINGLE_WILDCARD: Token = Token("*");

pub const SINGLE_WILDCARD_CHAR: char = '*';

/// Wildcard matching all following [`Token`]s.
///
/// Only valid as last token of a [`Subject`].
pub const MULTI_WILDCARD: Token = Token(">");

pub const MULTI_WILDCARD_CHAR: char = '>';

/// Separator of [`Token`]s.
pub const TOKEN_SEPARATOR: char = '.';

/// Errors validating a NATS subject.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("NATS subjects only support tokens in the form of ^([0-9a-zA-Z]+ | \\* | >)$")]
    InvalidToken,
    #[error("The multi wildcard '>' is only allowed at the end of a subject")]
    MultiWildcardInMiddle,
    #[error("The separator '.' is not allowed at the end or beginning of a subject")]
    SeparatorAtEndOrBeginning,
    #[error("Could not join on a subject ending with the multi wildcard")]
    CanNotJoin,
}

/// A valid NATS subject.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Subject<'s>(&'s str);

/// An owned, valid NATS subject.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubjectBuf(String);

/// A valid token of a NATS [`Subject`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Token<'t>(&'t str);

/// Iterator over a [`Subject`]'s tokens.
#[derive(Debug, Clone)]
pub struct Tokens<'s> {
    remaining_subject: &'s str,
}

impl<'s> Subject<'s> {
    /// Create a new, validated NATS subject.
    // [`FromStr`] does not allow Self to borrow from the input string.
    #[allow(clippy::should_implement_trait)]
    pub fn from_str(subject: &'s str) -> Result<Self, Error> {
        match subject {
            s if s.is_empty() => Err(Error::InvalidToken),
            s if s.starts_with(TOKEN_SEPARATOR) || s.ends_with(TOKEN_SEPARATOR) => {
                Err(Error::SeparatorAtEndOrBeginning)
            }
            s if s[..(s.len() - 1)].contains(MULTI_WILDCARD_CHAR) => {
                Err(Error::MultiWildcardInMiddle)
            }
            s if s
                .split(TOKEN_SEPARATOR)
                .any(|t| Token::from_str(t).is_err()) =>
            {
                Err(Error::InvalidToken)
            }
            _ => Ok(Self(subject)),
        }
    }
    /// The subject as `&str`.
    pub fn as_str(&self) -> &str {
        self.as_ref()
    }
    /// Iterate over the subject's [`Token`]s.
    pub fn tokens(&self) -> Tokens {
        self.into_iter()
    }
    /// Get an owned version of the subject.
    pub fn to_owned(&self) -> SubjectBuf {
        SubjectBuf(self.0.to_owned())
    }
    /// Check if two subjects match, considering wildcards.
    pub fn matches(&self, other: &Subject) -> bool {
        for (s, o) in self.tokens().zip(other.tokens()) {
            if s.is_multi_wildcard() || o.is_multi_wildcard() {
                return true;
            }
            if !s.matches(&o) {
                return false;
            }
        }
        true
    }

    /// Check if the subject contains any wildcards.
    ///
    /// _Note:_ You can't publish to a subject that contains a wildcard.
    pub fn contains_wildcards(&self) -> bool {
        self.0.contains(SINGLE_WILDCARD_CHAR) | self.0.contains(MULTI_WILDCARD_CHAR)
    }
}

impl<'s> AsRef<str> for Subject<'s> {
    fn as_ref(&self) -> &str {
        self.0
    }
}

impl<'s> IntoIterator for &'s Subject<'s> {
    type Item = Token<'s>;
    type IntoIter = Tokens<'s>;

    fn into_iter(self) -> Self::IntoIter {
        Tokens {
            remaining_subject: self.0,
        }
    }
}

impl<'s> PartialEq<&'s str> for Subject<'s> {
    fn eq(&self, other: &&'s str) -> bool {
        self.as_str() == *other
    }
}

impl<'s> fmt::Display for Subject<'s> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl SubjectBuf {
    /// Create a new, owned and validated NATS subject.
    pub fn new(subject: String) -> Result<Self, Error> {
        Subject::from_str(&subject)?;
        Ok(Self(subject))
    }
    /// Convert the subject buffer into the inner string.
    pub fn into_inner(self) -> String {
        self.0
    }
    /// The subject as `&str`.
    pub fn as_str(&self) -> &str {
        &self.0
    }
    /// Get the immutable reference type of a subject.
    pub fn as_ref(&self) -> Subject {
        Subject(&self.0)
    }
    /// Iterate over the subject's [`Token`]s.
    pub fn tokens(&self) -> Tokens {
        Tokens {
            remaining_subject: &self.0,
        }
    }
    /// Append a token.
    pub fn join(mut self, token: Token) -> Result<Self, Error> {
        if self.0.ends_with(MULTI_WILDCARD_CHAR) {
            Err(Error::CanNotJoin)
        } else {
            let token = token.as_str();
            self.0.reserve(token.len() + 1);
            self.0.push(TOKEN_SEPARATOR);
            self.0.push_str(token);
            Ok(self)
        }
    }
    /// Append a string. If the string is not a valid token an [`Error`] is returned.
    pub fn join_str(self, token: &str) -> Result<Self, Error> {
        let token = Token::from_str(token)?;
        self.join(token)
    }

    /// Check if the subject contains any wildcards.
    ///
    /// _Note:_ You can't publish to a subject that contains a wildcard.
    pub fn contains_wildcards(&self) -> bool {
        self.as_ref().contains_wildcards()
    }
}

impl FromStr for SubjectBuf {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Subject::from_str(s)?;
        Ok(SubjectBuf(s.to_owned()))
    }
}

impl<'o> PartialEq<&'o str> for SubjectBuf {
    fn eq(&self, other: &&'o str) -> bool {
        self.as_str() == *other
    }
}

impl fmt::Display for SubjectBuf {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_ref())
    }
}

impl<'t> Token<'t> {
    /// Create a new validated token.
    // [`FromStr`] does not allow Self to borrow from the input string.
    #[allow(clippy::should_implement_trait)]
    pub fn from_str(token: &'t str) -> Result<Self, Error> {
        match token {
            "*" | ">" => Ok(Self(token)),
            s if !s.is_empty() && s.chars().all(|c| c.is_ascii_alphanumeric()) => Ok(Self(token)),
            _ => Err(Error::InvalidToken),
        }
    }
    /// Const constructor for a token.
    ///
    /// # WARNING
    ///
    /// An invalid token may brake assumptions of the [`Token`] type. Reassure,
    /// that this call definitely constructs a valid token.
    pub const fn new_unchecked(token: &'t str) -> Self {
        Token(token)
    }
    /// The token as a `&str`
    pub fn as_str(&self) -> &str {
        self.as_ref()
    }
    /// Check if the token is the multi wildcard `>`.
    pub fn is_multi_wildcard(&self) -> bool {
        self == &MULTI_WILDCARD
    }
    /// Check if two tokens match, considering wildcards.
    pub fn matches(&self, other: &Token) -> bool {
        match (self, other) {
            (&SINGLE_WILDCARD, _)
            | (_, &SINGLE_WILDCARD)
            | (&MULTI_WILDCARD, _)
            | (_, &MULTI_WILDCARD) => true,
            (l, r) => l == r,
        }
    }
}

impl<'t> AsRef<str> for Token<'t> {
    fn as_ref(&self) -> &str {
        self.0
    }
}

impl<'t> PartialEq<&'t str> for Token<'t> {
    fn eq(&self, other: &&'t str) -> bool {
        self.as_str() == *other
    }
}

impl<'t> fmt::Display for Token<'t> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl<'s> Iterator for Tokens<'s> {
    type Item = Token<'s>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining_subject.is_empty() {
            None
        } else if self.remaining_subject.contains(TOKEN_SEPARATOR) {
            let (token, rest) = self.remaining_subject.split_once(TOKEN_SEPARATOR)?;
            self.remaining_subject = rest;
            Some(Token(token))
        } else {
            let last = std::mem::replace(&mut self.remaining_subject, "");
            Some(Token(last))
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use test_case::test_case;

    #[test_case("" => false        ; "empty")]
    #[test_case("*" => true        ; "single wildcard")]
    #[test_case(">" => true        ; "multi wildcard")]
    #[test_case("!" => false       ; "special char")]
    #[test_case("á" => false       ; "non ascii")]
    #[test_case("probe" => true    ; "valid name")]
    #[test_case("pröbe" => false   ; "non alphanumeric")]
    #[test_case("PrObE007" => true ; "wild stuff")]
    fn validate_token(token: &str) -> bool {
        Token::from_str(token).is_ok()
    }

    #[test_case("" => false               ; "empty")]
    #[test_case("*" => true               ; "single wildcard")]
    #[test_case(">" => true               ; "wire tap")]
    #[test_case("abc.12345.cda.>" => true ; "end with multi")]
    #[test_case("uu.12345" => true        ; "plain")]
    #[test_case("fAN.*.sdb.*" => true     ; "multiple single wildcards")]
    #[test_case("zzz.>.cdc" => false      ; "middle multi wildcard")]
    #[test_case("zzz.*." => false         ; "ending dot")]
    #[test_case(".dot" => false           ; "starting dot")]
    #[test_case(">>" => false             ; "double multi wildcard")]
    #[test_case("hi.**.no" => false       ; "double single wildcard")]
    fn validate_subject(subject: &str) -> bool {
        Subject::from_str(subject).is_ok()
    }

    #[test_case("*", "abc" => true    ; "single wildcard")]
    #[test_case("cba", "*" => true    ; "single wildcard reverse")]
    #[test_case(">", "abc" => true    ; "multi wildcard")]
    #[test_case("cba", ">" => true    ; "multi wildcard reverse")]
    #[test_case("*", ">" => true      ; "mixed wildcards")]
    #[test_case("cba", "abc" => false ; "unequal tokens")]
    fn match_tokens(l: &str, r: &str) -> bool {
        let l = Token::from_str(l).unwrap();
        let r = Token::from_str(r).unwrap();
        l.matches(&r)
    }

    #[test_case("cba", "abc" => false               ; "unequal subjects")]
    #[test_case("cba.*", "cba.abc" => true          ; "single wildcard")]
    #[test_case("cba.*.zzz", "cba.abc.zzz" => true  ; "single wildcard middle")]
    #[test_case(">", "cba.abc.zzz" => true          ; "wire tap")]
    #[test_case(">", "cba.*.zzz" => true            ; "wire tap against single wildcard")]
    #[test_case("cba.>", "cba.abc.zzz" => true      ; "multi wildcard")]
    #[test_case("*.>", "cba.abc.zzz" => true        ; "both wildcards")]
    #[test_case("cba.*.zzz", "cba.abc.yyy" => false ; "not matching")]
    fn match_subjects(l: &str, r: &str) -> bool {
        let l = Subject::from_str(l).unwrap();
        let r = Subject::from_str(r).unwrap();
        l.matches(&r)
    }

    #[test_case("abc", &["def"], "abc.def"                       ; "single token")]
    #[test_case("abc", &["def", "ghi", "012"], "abc.def.ghi.012" ; "more tokens")]
    #[test_case(">", &["abc"], "" => panics                      ; "wire tap")]
    #[test_case("abc.def.>", &["abc"], "" => panics              ; "join on multi wildcard")]
    #[test_case("abc.def", &["*"], "abc.def.*"                   ; "single wildcard")]
    #[test_case("abc.def", &["*", "fed"], "abc.def.*.fed"        ; "single wildcard and more")]
    #[test_case("abc", &[">"], "abc.>"                           ; "multi wildcard")]
    #[test_case("abc", &[">", "cba"], "" => panics               ; "multi wildcard and more")]
    fn join_subject(base: &str, appends: &[&str], expect: &str) {
        let mut base = base.parse::<SubjectBuf>().unwrap();
        for append in appends {
            base = base.join_str(append).unwrap();
        }

        assert_eq!(base, expect);
    }
}
