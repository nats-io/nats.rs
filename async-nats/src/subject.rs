use bytes::Bytes;
use std::ops::Deref;
use std::str::{from_utf8, Utf8Error};

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Subject {
    bytes: Bytes,
}

impl Subject {
    pub fn from_static(input: &'static str) -> Self {
        Subject {
            bytes: Bytes::from_static(input.as_bytes()),
        }
    }

    pub fn from_utf8(vec: Vec<u8>) -> Result<Self, Utf8Error> {
        let utf8_str = from_utf8(&vec)?;
        Ok(Subject {
            bytes: Bytes::from(vec),
        })
    }
}

impl Deref for Subject {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        // SAFETY: It is safe to perform an unchecked conversion from bytes to a string slice
        // here because we guarantee that the bytes in the `Subject` struct are valid UTF-8.
        // This is enforced during the construction of `Subject` through the `from_static`,
        // and `from_utf8` methods. In both cases, the input is either checked for UTF-8 validity or
        // known to be valid UTF-8 as a static string.
        unsafe { std::str::from_utf8_unchecked(&self.bytes) }
    }
}

#[cfg(test)]
mod tests {
    use super::Subject;

    #[test]
    fn test_from_static() {
        let static_input = "Static string";
        let subject = Subject::from_static(static_input);
        assert_eq!(&*subject, "Static string");
    }

    #[test]
    fn test_from_utf8() {
        let utf8_input = vec![72, 101, 108, 108, 111]; // "Hello" in UTF-8
        let subject = Subject::from_utf8(utf8_input).unwrap();
        assert_eq!(&*subject, "Hello");
    }

    #[test]
    fn test_from_utf8_invalid_utf8() {
        let invalid_input = vec![0xC3, 0x28];
        let subject = Subject::from_utf8(invalid_input);
        assert!(subject.is_err());
    }
}
