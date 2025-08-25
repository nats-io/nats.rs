use async_nats::subject::SubjectError;
use async_nats::Subject;

// Compile-time validation tests - these should compile successfully
const VALID1: Subject = Subject::from_static_validated("events.data");
const VALID2: Subject = Subject::from_static_validated("foo.bar.baz");
const VALID3: Subject = Subject::from_static_validated("a");
const VALID4: Subject = Subject::from_static_validated("foo.*.bar");
const VALID5: Subject = Subject::from_static_validated("foo.>");

// Compile-time validation failures - uncomment to verify they fail at compile time:
// const INVALID_SPACE: Subject = Subject::from_static_validated("invalid subject");
// const INVALID_EMPTY: Subject = Subject::from_static_validated("");
// const INVALID_START_DOT: Subject = Subject::from_static_validated(".invalid");
// const INVALID_END_DOT: Subject = Subject::from_static_validated("invalid.");

#[test]
fn test_const_validated_subjects() {
    assert_eq!(VALID1.as_str(), "events.data");
    assert_eq!(VALID2.as_str(), "foo.bar.baz");
    assert_eq!(VALID3.as_str(), "a");
    assert_eq!(VALID4.as_str(), "foo.*.bar");
    assert_eq!(VALID5.as_str(), "foo.>");
}

#[test]
fn test_valid_subjects() {
    assert!(Subject::validated("events.data").is_ok());
    assert!(Subject::validated("foo.bar.baz").is_ok());
    assert!(Subject::validated("a").is_ok());
    assert!(Subject::validated("foo.*.bar").is_ok());
    assert!(Subject::validated("foo.>").is_ok());
    assert!(Subject::validated("_INBOX.123").is_ok());
}

#[test]
fn test_invalid_subjects_with_space() {
    assert!(matches!(
        Subject::validated("events data"),
        Err(SubjectError::InvalidFormat)
    ));
}

#[test]
fn test_invalid_subjects_with_tab() {
    assert!(matches!(
        Subject::validated("events\tdata"),
        Err(SubjectError::InvalidFormat)
    ));
}

#[test]
fn test_invalid_subjects_with_newline() {
    assert!(matches!(
        Subject::validated("events\ndata"),
        Err(SubjectError::InvalidFormat)
    ));
}

#[test]
fn test_invalid_subjects_with_carriage_return() {
    assert!(matches!(
        Subject::validated("events\rdata"),
        Err(SubjectError::InvalidFormat)
    ));
}

#[test]
fn test_invalid_subjects_starts_with_dot() {
    assert!(matches!(
        Subject::validated(".events"),
        Err(SubjectError::InvalidFormat)
    ));
}

#[test]
fn test_invalid_subjects_ends_with_dot() {
    assert!(matches!(
        Subject::validated("events."),
        Err(SubjectError::InvalidFormat)
    ));
}

#[test]
fn test_invalid_subjects_with_consecutive_dots() {
    assert!(matches!(
        Subject::validated("events..data"),
        Err(SubjectError::InvalidFormat)
    ));
    assert!(matches!(
        Subject::validated("a..b..c"),
        Err(SubjectError::InvalidFormat)
    ));
}

#[test]
fn test_invalid_empty_subject() {
    assert!(matches!(
        Subject::validated(""),
        Err(SubjectError::InvalidFormat)
    ));
}

#[test]
fn test_validated_subject_clone() {
    let validated = Subject::validated("test.subject").unwrap();
    let cloned = validated.clone();

    assert_eq!(validated.as_str(), cloned.as_str());
}

#[test]
fn test_validated_subject_display() {
    let validated = Subject::validated("test.subject").unwrap();
    assert_eq!(format!("{}", validated), "test.subject");
}

#[test]
fn test_subject_is_valid() {
    let valid = Subject::from("events.data");
    assert!(valid.is_valid());

    let invalid = Subject::from("invalid subject");
    assert!(!invalid.is_valid());

    let empty = Subject::from("");
    assert!(!empty.is_valid());
}
