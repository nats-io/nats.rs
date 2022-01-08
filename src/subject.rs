// Copyright 2022 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

trait Subject {
    fn is_valid(&self) -> bool;
    fn tokens(&self) -> std::io::Result<Vec<&str>>;
    fn token_at(&self, n: usize) -> std::io::Result<&str>;
}

impl Subject for &str {
    fn is_valid(&self) -> bool {
        let mut fwc_seen = false;
        for token in self.split('.') {
            if !token_valid(token) || fwc_seen {
                return false;
            }
            fwc_seen = token_is_fwc(token);
        }
        true
    }

    fn tokens(&self) -> std::io::Result<Vec<&str>> {
        let tokens = self.split('.').collect::<Vec<&str>>();
        let mut fwc_seen = false;
        for (i, token) in tokens.iter().enumerate() {
            if !token_valid(token) || fwc_seen {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!("invalid token at position {}", i),
                ));
            }
            fwc_seen = token_is_fwc(token);
        }
        Ok(tokens)
    }

    fn token_at(&self, n: usize) -> std::io::Result<&str> {
        let tokens = self.tokens()?;
        if n >= tokens.len() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("no token at position {}", n),
            ));
        }
        Ok(tokens[n])
    }
}

fn token_valid(token: &str) -> bool {
    let never_allowed: &[char] = &[' ', '\n', '\t', '\r', '.'];
    if token.is_empty() || token.contains(never_allowed) {
        return false;
    }
    if token.len() == 1 {
        return true;
    }
    let wildcards: &[char] = &['*', '>'];
    !token.contains(wildcards)
}

#[inline]
fn token_is_fwc(token: &str) -> bool {
    token.len() == 1 && &token[0..1] == ">"
}

#[test]
fn token_is_valid() {
    assert!(token_valid("foo"));
    assert!(token_valid("*"));
    assert!(token_valid(">"));

    assert_eq!(token_valid(".foo"), false);
    assert_eq!(token_valid("foo."), false);
    assert_eq!(token_valid("f o"), false);
    assert_eq!(token_valid("f\no"), false);
    assert_eq!(token_valid("f*o"), false);
    assert_eq!(token_valid("f>o"), false);
    assert_eq!(token_valid("f>"), false);
}

#[test]
fn token_is_full_wildcard() {
    assert!(token_is_fwc(">"));
    assert_eq!(token_is_fwc("*"), false);
    assert_eq!(token_is_fwc("foo"), false);
    assert_eq!(token_is_fwc(">>"), false);
}

#[test]
fn subject_is_valid() {
    assert!("foo".is_valid());
    assert!("foo.*".is_valid());
    assert!("foo.>".is_valid());

    assert!("foo.".is_valid() == false);
    assert!(".foo".is_valid() == false);
    assert!("foo.>.bar".is_valid() == false);

    // Make sure String version works too.
    let s = String::from("foo");
    assert!(s.as_str().is_valid());
}

#[test]
fn tokens() {
    assert_eq!("foo".tokens().unwrap(), ["foo"]);
    assert_eq!("foo.bar".tokens().unwrap(), ["foo", "bar"]);
    assert!("foo.b r".tokens().is_err());
    assert!("foo.>.>".tokens().is_err());

    // Make sure String version works too.
    let s = String::from("foo.bar");
    assert_eq!(s.as_str().tokens().unwrap(), ["foo", "bar"]);
}

#[test]
fn tokens_nth() {
    assert_eq!("foo.bar".token_at(1).unwrap(), "bar");
    assert_eq!("foo.bar".token_at(0).unwrap(), "foo");
    assert!("foo.bar".token_at(2).is_err());

    // Make sure String version works too.
    let s = String::from("foo.bar");
    assert_eq!(s.as_str().token_at(1).unwrap(), "bar");
}
