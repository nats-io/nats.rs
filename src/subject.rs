//! Typed implementation of a NATS subject.

use std::{
    borrow::Borrow,
    convert::TryFrom,
    fmt,
    hash::{Hash, Hasher},
    io,
    ops::Deref,
    str::FromStr,
};

use serde::{Deserialize, Serialize};

/// Wildcard matching a single token.
pub const SINGLE_WILDCARD: &str = "*";

/// Wildcard matching all following tokens.
///
/// Only valid as last token of a [`Subject`].
pub const MULTI_WILDCARD: &str = ">";

/// The character marking a multi wildcard
pub const MULTI_WILDCARD_CHAR: char = '>';

/// Separator of [`Token`]s.
pub const TOKEN_SEPARATOR: char = '.';

/// Errors validating a NATS subject.
#[derive(Debug, Copy, Clone, thiserror::Error)]
pub enum Error {
    /// One of the [`Subject`]'s token is invalid.
    #[error("NATS subjects's tokens are not allowed to be empty or to contain spaces or dots")]
    InvalidToken,
    /// The multi-wildcard token `>` is used within or at the beginning of a [`Subject`].
    #[error("The multi wildcard '>' is only allowed at the end of a subject")]
    MultiWildcardInMiddle,
    /// The [`Subject`] started or ended with a `.`.
    #[error("The separator '.' is not allowed at the end or beginning of a subject")]
    SeparatorAtEndOrBeginning,
    /// Can not join [`Subject`] as it ends with a multi-wildcard as this would result in an invalid
    /// [`Subject`].
    #[error("Could not join on a subject ending with the multi wildcard")]
    CanNotJoin,
}

impl From<Error> for io::Error {
    fn from(err: Error) -> Self {
        io::Error::new(io::ErrorKind::InvalidInput, err)
    }
}

/// Convert something into a [`Subject`].
pub trait AsSubject {
    /// Try to represent as a [`Subject`].
    fn as_subject(&self) -> Result<&Subject, io::Error>;
}

/// A valid NATS subject.
#[repr(transparent)]
#[derive(Debug, Eq)]
pub struct Subject(str);

/// An owned, valid NATS subject.
#[derive(Debug, Clone, Eq, Serialize, Deserialize)]
#[serde(try_from = "String")]
#[serde(into = "String")]
pub struct SubjectBuf(String);

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
        #[allow(unsafe_code)]
        #[allow(trivial_casts)]
        unsafe {
            let ptr = sub as *const _ as *const Self;
            &*ptr
        }
    }
    /// Create a new, validated NATS subject.
    pub fn new(subject: &str) -> Result<&Self, Error> {
        match subject.as_bytes() {
            b"" => Err(Error::InvalidToken),
            [b'.', ..] | [.., b'.'] => Err(Error::SeparatorAtEndOrBeginning),
            s if s.starts_with(b">.") || s.windows(3).any(|win| win == b".>.") => {
                Err(Error::MultiWildcardInMiddle)
            }
            s if s.windows(2).any(|win| win == b"..") => Err(Error::InvalidToken),
            s if s.iter().any(|b| b" \t\n\r".contains(b)) => Err(Error::InvalidToken),
            _ => Ok(()),
        }?;

        Ok(Self::new_unchecked(subject))
    }
    /// The subject as `&str`.
    pub fn as_str(&self) -> &str {
        self.deref()
    }
    /// Iterate over the subject's [`Token`]s.
    pub fn tokens(&self) -> Tokens<'_> {
        self.into_iter()
    }
    /// Check if two subjects match, considering wildcards.
    pub fn matches(&self, other: &Subject) -> bool {
        let mut s_tokens = self.tokens();
        let mut o_tokens = other.tokens();

        loop {
            match (s_tokens.next(), o_tokens.next()) {
                (Some(MULTI_WILDCARD), Some(_)) | (Some(_), Some(MULTI_WILDCARD)) => break true,
                (Some(s_t), Some(o_t)) => {
                    if token_match(s_t, o_t) {
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
        self.tokens()
            .any(|t| t == SINGLE_WILDCARD || t == MULTI_WILDCARD)
    }
    /// Get the nth token of the subject.
    ///
    /// Returns `None` if there are not enough tokens.
    pub fn get_token(&self, idx: usize) -> Option<&str> {
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
    type Item = &'s str;
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

impl PartialEq for Subject {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
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

impl AsSubject for Subject {
    fn as_subject(&self) -> Result<&Subject, io::Error> {
        Ok(self)
    }
}

impl<'s> AsSubject for &'s Subject {
    fn as_subject(&self) -> Result<&Subject, io::Error> {
        Ok(self)
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
    pub fn join(mut self, token: &str) -> Result<Self, Error> {
        if !valid_token(token) {
            Err(Error::InvalidToken)
        } else if self.0.ends_with(MULTI_WILDCARD_CHAR) {
            Err(Error::CanNotJoin)
        } else {
            self.0.reserve(token.len() + 1);
            self.0.push(TOKEN_SEPARATOR);
            self.0.push_str(token);
            Ok(self)
        }
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

impl PartialEq for SubjectBuf {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
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

impl AsSubject for SubjectBuf {
    fn as_subject(&self) -> Result<&Subject, io::Error> {
        Ok(self.deref())
    }
}

impl<'sb> AsSubject for &'sb SubjectBuf {
    fn as_subject(&self) -> Result<&Subject, io::Error> {
        Ok(self.deref())
    }
}

impl<'s> Iterator for Tokens<'s> {
    type Item = &'s str;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining_subject.is_empty() {
            None
        } else if self.remaining_subject.contains(TOKEN_SEPARATOR) {
            let (token, rest) = self.remaining_subject.split_once(TOKEN_SEPARATOR)?;
            self.remaining_subject = rest;
            Some(token)
        } else {
            let last = std::mem::take(&mut self.remaining_subject);
            Some(last)
        }
    }
}

impl AsSubject for str {
    fn as_subject(&self) -> Result<&Subject, io::Error> {
        Subject::new(self).map_err(Into::into)
    }
}

impl<'s> AsSubject for &'s str {
    fn as_subject(&self) -> Result<&Subject, io::Error> {
        (*self).as_subject()
    }
}

impl AsSubject for String {
    fn as_subject(&self) -> Result<&Subject, io::Error> {
        self.as_str().as_subject()
    }
}

impl<'s> AsSubject for &'s String {
    fn as_subject(&self) -> Result<&Subject, io::Error> {
        (*self).as_subject()
    }
}

fn valid_token(token: &str) -> bool {
    !token.is_empty() && !token.contains(['.', ' ', '\n', '\t', '\r'])
}

fn token_match(lt: &str, rt: &str) -> bool {
    lt == rt
        || lt == SINGLE_WILDCARD
        || rt == SINGLE_WILDCARD
        || lt == MULTI_WILDCARD
        || rt == MULTI_WILDCARD
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
        valid_token(token)
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
    #[test_case("dot..dot" => false       ; "empty token")]
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
        token_match(l, r)
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
            base = base.join(append).unwrap();
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
        sub.sub_subject(start, end)
            .map(|sub| sub.as_str() == expect)
            .unwrap_or(false)
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
