//! Typed implementation of a NATS subject.

use std::{
    fmt, 
    str::FromStr, 
    io, 
    ops::Deref,
    hash::{Hash, Hasher}, borrow::Borrow, convert::TryFrom,
};

use serde::{Serialize, Deserialize};

lazy_static::lazy_static!{
    /// Wildcard matching a single [`Token`].
    pub static ref SINGLE_WILDCARD: &'static Token = Token::new_unchecked("*");
    
    /// Wildcard matching all following [`Token`]s.
    ///
    /// Only valid as last token of a [`Subject`].
    pub static ref MULTI_WILDCARD: &'static Token = Token::new_unchecked(">");
}

// /// The character marking a single wildcard
// pub const SINGLE_WILDCARD_CHAR: char = '*';

/// The character marking a multi wildcard
pub const MULTI_WILDCARD_CHAR: char = '>';

/// Separator of [`Token`]s.
pub const TOKEN_SEPARATOR: char = '.';

/// Errors validating a NATS subject.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("NATS subjects's tokens are not allowed to be empty or to contain spaces or dots")]
    InvalidToken,
    #[error("The multi wildcard '>' is only allowed at the end of a subject")]
    MultiWildcardInMiddle,
    #[error("The separator '.' is not allowed at the end or beginning of a subject")]
    SeparatorAtEndOrBeginning,
    #[error("Could not join on a subject ending with the multi wildcard")]
    CanNotJoin,
}

impl From<Error> for io::Error {
    fn from(err: Error) -> Self {
        io::Error::new(io::ErrorKind::InvalidInput, err)
    }
}

/// A valid NATS subject.
#[repr(transparent)]
#[derive(Debug, PartialEq, Eq)]
pub struct Subject(str);

/// An owned, valid NATS subject.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "String")]
#[serde(into = "String")]
pub struct SubjectBuf(String);

/// A valid token of a NATS [`Subject`].
#[repr(transparent)]
#[derive(Debug, PartialEq, Eq)]
pub struct Token(str);

/// Iterator over a [`Subject`]'s tokens.
#[derive(Debug, Clone)]
pub struct Tokens<'s> {
    remaining_subject: &'s str,
}

impl Subject {
    /// Constructor for a subject.
    ///
    /// # WARNING
    ///
    /// An invalid token may brake assumptions of the [`Subject`] type. Reassure, that this call
    /// definitely constructs a valid subject.
    pub fn new_unchecked(sub: &str) -> &Self {
        // Safety: Subject is #[repr(transparent)] therefore this is okay
        unsafe { 
            let ptr = sub as *const _ as *const Self; 
            &*ptr
        }
    }
    /// Create a new, validated NATS subject.
    pub fn new(subject: &str) -> Result<&Self, Error> {
        let subject = match subject {
            s if s.is_empty() => Err(Error::InvalidToken),
            s if s.starts_with(TOKEN_SEPARATOR) || s.ends_with(TOKEN_SEPARATOR) => {
                Err(Error::SeparatorAtEndOrBeginning)
            }
            _ => Ok(subject)
        }?;
        let mut last_was_multi_wildcard = false;
        for token in subject.split(TOKEN_SEPARATOR) {
            if last_was_multi_wildcard {
                return Err(Error::MultiWildcardInMiddle);
            }
            let token = Token::new(token)?;
            last_was_multi_wildcard = token == *MULTI_WILDCARD;
        }
        Ok(Self::new_unchecked(subject))
    }
    /// The subject as `&str`.
    pub fn as_str(&self) -> &str {
        self.deref()
    }
    /// Iterate over the subject's [`Token`]s.
    pub fn tokens(&self) -> Tokens {
        self.into_iter()
    }
    /// Check if two subjects match, considering wildcards.
    pub fn matches(&self, other: &Subject) -> bool {
        let mut s_tokens = self.tokens();
        let mut o_tokens = other.tokens();

        loop {
            match (s_tokens.next(), o_tokens.next()) {
                (Some(mw), Some(_)) | (Some(_), Some(mw)) if mw == *MULTI_WILDCARD => break true,
                (Some(s_t), Some(o_t)) => {
                    if s_t.matches(&o_t) {
                        continue;
                    } else {
                        break false;
                    }
                }
                (None, Some(_)) | (Some(_), None) => break false,
                (None, None) => break true,
            }
        }
    }
    /// Check if the subjects ends with a multi wildcard.
    pub fn ends_with_multi_wildcard(&self) -> bool {
        self.ends_with(MULTI_WILDCARD_CHAR)
    }
    /// Check if the subject contains any wildcards.
    ///
    /// _Note:_ You can't publish to a subject that contains a wildcard.
    pub fn contains_wildcards(&self) -> bool {
        self.tokens().any(|t| t.is_wildcard())
    }
    /// Get the nth token of the subject.
    ///
    /// Returns `None` if there are not enough tokens.
    pub fn get_token(&self, idx: usize) -> Option<&Token> {
        self.tokens().nth(idx)
    }
    /// Get a sub-subject from the subject.
    ///
    /// # Example
    /// ```
    /// let sub = nats::Subject::new("abc.def.ghi")?;
    /// let sub_sub = sub.sub_subject(0, 1).unwrap();
    /// assert_eq!(sub_sub.as_str(), "abc.def");
    /// # Ok::<(), nats::SubjectError>(())
    /// ```
    pub fn sub_subject(&self, start_token: usize, end_token: usize) -> Option<&Subject> {
        let tokens_cnt = self.0.split(TOKEN_SEPARATOR).count();
        if start_token >= tokens_cnt || end_token >= tokens_cnt || start_token > end_token {
            return None;
        }

        let mut separators = self.match_indices(TOKEN_SEPARATOR).map(|(idx, _)| idx);
        let start_idx = if start_token == 0 {
            0
        } else {
            // Minus first token, it doesn't start with a '.'
            // idx + 1 to not fetch the '.'
            separators.nth(start_token - 1)? + 1
        };
        let end_idx = if end_token == tokens_cnt - 1 {
            self.len() - 1
        } else {
            separators.nth(end_token - start_token)? - 1
        };

        Some(Subject::new_unchecked(&self.0[start_idx..=end_idx]))
    }
}

impl AsRef<str> for Subject {
    fn as_ref(&self) -> &str {
        self.deref()
    }
}

impl<'s> IntoIterator for &'s Subject {
    type Item = &'s Token;
    type IntoIter = Tokens<'s>;

    fn into_iter(self) -> Self::IntoIter {
        Tokens {
            remaining_subject: &self.0,
        }
    }
}

impl PartialEq<str> for Subject {
    fn eq(&self, other: &str) -> bool {
        self.as_str() == other
    }
}

impl fmt::Display for Subject {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl Deref for Subject {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Hash for Subject {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.as_str().hash(state);
    }
}

impl ToOwned for Subject {
    type Owned = SubjectBuf;

    fn to_owned(&self) -> Self::Owned {
        SubjectBuf(self.0.to_owned())
    }
}

impl SubjectBuf {
    /// Create a new, owned and validated NATS subject.
    pub fn new(subject: String) -> Result<Self, Error> {
        Subject::new(&subject)?;
        Ok(Self(subject))
    }
    /// Const constructor for a subject buffer without validation.
    ///
    /// # WARNING
    ///
    /// An invalid subject may brake assumptions of the [`SubjectBuf`] type. Reassure, that this call
    /// definitely constructs a valid subject buffer.
    pub const fn new_unchecked(subject: String) -> Self {
        Self(subject)
    }
    /// Convert the subject buffer into the inner string.
    pub fn into_inner(self) -> String {
        self.0
    }
    /// Append a token.
    pub fn join(mut self, token: &Token) -> Result<Self, Error> {
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
        let token = Token::new(token)?;
        self.join(token)
    }
}

impl FromStr for SubjectBuf {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Subject::new(s)?;
        Ok(SubjectBuf(s.to_owned()))
    }
}

impl From<SubjectBuf> for String {
    fn from(sub: SubjectBuf) -> Self {
        sub.0
    }
}

impl TryFrom<String> for SubjectBuf {
    type Error = Error;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl PartialEq<str> for SubjectBuf {
    fn eq(&self, other: &str) -> bool {
        self.as_str() == other
    }
}

impl<'s> PartialEq<&'s str> for SubjectBuf {
    fn eq(&self, other: &&'s str) -> bool {
        self.as_str() == *other
    }
}

impl fmt::Display for SubjectBuf {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_ref())
    }
}

impl Deref for SubjectBuf {
    type Target = Subject;

    fn deref(&self) -> &Self::Target {
        Subject::new_unchecked(&self.0)
    }
}

impl AsRef<Subject> for SubjectBuf {
    fn as_ref(&self) -> &Subject {
        self.deref()
    }
}

impl Hash for SubjectBuf {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.as_str().hash(state);
    }
}

impl Borrow<Subject> for SubjectBuf {
    fn borrow(&self) -> &Subject {
        self.deref()
    }
}

impl Token {
    /// Constructor for a token.
    ///
    /// # WARNING
    ///
    /// An invalid token may brake assumptions of the [`Token`] type. Reassure, that this call
    /// definitely constructs a valid token.
    pub fn new_unchecked(token: &str) -> &Self {
        // Safety: Token is #[repr(transparent)] therefore this is okay
        unsafe { 
            let ptr = token as *const _ as *const Self;
            &*ptr
        }
    }
    /// Create a new validated token.
    pub fn new(token: &str) -> Result<&Self, Error> {
        if token.is_empty() || token.chars().any(|c| c == TOKEN_SEPARATOR || c == ' ') {
            Err(Error::InvalidToken)
        } else {
            Ok(Self::new_unchecked(token))
        }
    }
    /// The token as a `&str`
    pub fn as_str(&self) -> &str {
        self.deref()
    }
    /// Check if the token is the multi wildcard `>`.
    pub fn is_wildcard(&self) -> bool {
        self == *MULTI_WILDCARD || self == *SINGLE_WILDCARD
    }
    /// Check if two tokens match, considering wildcards.
    pub fn matches(&self, other: &Token) -> bool {
        match (self, other) {
            (t, _)
            | (_, t) if t == *MULTI_WILDCARD || t == *SINGLE_WILDCARD => true,
            (l, r) => l == r,
        }
    }
}

impl AsRef<str> for Token {
    fn as_ref(&self) -> &str {
        self.deref()
    }
}

impl PartialEq<str> for Token {
    fn eq(&self, other: &str) -> bool {
        self.as_str() == other
    }
}

impl fmt::Display for Token {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl Deref for Token {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<'s> Iterator for Tokens<'s> {
    type Item = &'s Token;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining_subject.is_empty() {
            None
        } else if self.remaining_subject.contains(TOKEN_SEPARATOR) {
            let (token, rest) = self.remaining_subject.split_once(TOKEN_SEPARATOR)?;
            self.remaining_subject = rest;
            Some(Token::new_unchecked(token))
        } else {
            let last = std::mem::take(&mut self.remaining_subject);
            Some(Token::new_unchecked(last))
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
    #[test_case(">>" => true        ; "double multi wildcard")]
    #[test_case("!" => true        ; "special char")]
    #[test_case("á" => true        ; "non ascii")]
    #[test_case("probe" => true    ; "valid name")]
    #[test_case("pröbe" => true    ; "non alphanumeric")]
    #[test_case("$SYS" => true     ; "system account")]
    #[test_case("ab.cd" => false   ; "contains dot")]
    #[test_case("ab cd" => false   ; "contains space")]
    fn validate_token(token: &str) -> bool {
        Token::new(token).is_ok()
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
    #[test_case(">>" => true              ; "double multi wildcard")]
    #[test_case("hi.**.no" => true        ; "double single wildcard")]
    fn validate_subject(subject: &str) -> bool {
        Subject::new(subject).is_ok()
    }

    #[test_case("*", "abc" => true    ; "single wildcard")]
    #[test_case("cba", "*" => true    ; "single wildcard reverse")]
    #[test_case(">", "abc" => true    ; "multi wildcard")]
    #[test_case("cba", ">" => true    ; "multi wildcard reverse")]
    #[test_case("*", ">" => true      ; "mixed wildcards")]
    #[test_case("cba", "abc" => false ; "unequal tokens")]
    fn match_tokens(l: &str, r: &str) -> bool {
        let l = Token::new(l).unwrap();
        let r = Token::new(r).unwrap();
        l.matches(&r)
    }

    #[test_case("cba", "abc" => false               ; "unequal subjects")]
    #[test_case("cba.*", "cba.abc" => true          ; "single wildcard")]
    #[test_case("cba.*.zzz", "cba.abc.zzz" => true  ; "single wildcard middle")]
    #[test_case("ab.cd.ef", "ab.cd" => false        ; "longer")]
    #[test_case("ab.cd", "ab.cd.ef" => false        ; "longer reverse")]
    #[test_case(">", "cba.abc.zzz" => true          ; "wire tap")]
    #[test_case(">", "cba.*.zzz" => true            ; "wire tap against single wildcard")]
    #[test_case("cba.>", "cba.abc.zzz" => true      ; "multi wildcard")]
    #[test_case("*.>", "cba.abc.zzz" => true        ; "both wildcards")]
    #[test_case("cba.*.zzz", "cba.abc.yyy" => false ; "not matching")]
    fn match_subjects(l: &str, r: &str) -> bool {
        let l = Subject::new(l).unwrap();
        let r = Subject::new(r).unwrap();
        l.matches(r)
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
        let mut base = SubjectBuf::new(base.to_owned()).unwrap();
        for append in appends {
            base = base.join_str(append).unwrap();
        }

        assert_eq!(base, expect);
    }

    #[test_case("abc.def.ghi", 0, 1, "abc.def" => true      ; "simple")]
    #[test_case("abc.def.ghi", 0, 0, "abc" => true          ; "single beginning")]
    #[test_case("abc.def.ghi", 1, 1, "def" => true          ; "single middle")]
    #[test_case("abc.def.ghi", 2, 2, "ghi" => true          ; "single end")]
    #[test_case("abc.def.ghi", 0, 2, "abc.def.ghi" => true  ; "all")]
    #[test_case("abc.def.ghi", 0, 1, "abc.def" => true      ; "first two")]
    #[test_case("abc.def.ghi", 1, 2, "def.ghi" => true      ; "last two")]
    #[test_case("abc.def.ghi", 1,21, "" => false            ; "bound to high")]
    #[test_case("abc.def.ghi", 1, 0, "" => false            ; "start before end")]
    #[test_case("abc.def.ghi.jkl", 1, 2, "def.ghi" => true  ; "two middle")]
    fn sub_subjects(subject: &str, start: usize, end: usize, expect: &str) -> bool {
        let sub = Subject::new(subject).unwrap();
        sub.sub_subject(start, end).map(|sub| sub.as_str() == expect).unwrap_or(false)
    }

    #[test]
    fn same_hash() -> Result<(), Error> {
        let sub = Subject::new("foo.bar")?;
        let buf = sub.to_owned();
        let mut map = std::collections::HashSet::new();
        map.insert(buf);
        assert!(map.get(sub).is_some());
        Ok(())
    }
}
