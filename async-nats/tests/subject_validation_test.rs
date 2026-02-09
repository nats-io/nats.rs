use async_nats::subject::{SubjectError, ToSubject, ValidatedSubject};

// Compile-time validation tests - these should compile successfully
const VALID1: ValidatedSubject = ValidatedSubject::from_static("events.data");
const VALID2: ValidatedSubject = ValidatedSubject::from_static("foo.bar.baz");
const VALID3: ValidatedSubject = ValidatedSubject::from_static("a");
const VALID4: ValidatedSubject = ValidatedSubject::from_static("foo.*.bar");
const VALID5: ValidatedSubject = ValidatedSubject::from_static("foo.>");

// Compile-time validation failures - uncomment to verify they fail at compile time:
// const INVALID_SPACE: ValidatedSubject = ValidatedSubject::from_static("invalid subject");
// const INVALID_EMPTY: ValidatedSubject = ValidatedSubject::from_static("");
// const INVALID_START_DOT: ValidatedSubject = ValidatedSubject::from_static(".invalid");
// const INVALID_END_DOT: ValidatedSubject = ValidatedSubject::from_static("invalid.");

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
    assert!(ValidatedSubject::new("events.data").is_ok());
    assert!(ValidatedSubject::new("foo.bar.baz").is_ok());
    assert!(ValidatedSubject::new("a").is_ok());
    assert!(ValidatedSubject::new("foo.*.bar").is_ok());
    assert!(ValidatedSubject::new("foo.>").is_ok());
    assert!(ValidatedSubject::new("_INBOX.123").is_ok());
}

#[test]
fn test_invalid_subjects_with_space() {
    assert!(matches!(
        ValidatedSubject::new("events data"),
        Err(SubjectError::InvalidFormat)
    ));
}

#[test]
fn test_invalid_subjects_with_tab() {
    assert!(matches!(
        ValidatedSubject::new("events\tdata"),
        Err(SubjectError::InvalidFormat)
    ));
}

#[test]
fn test_invalid_subjects_with_newline() {
    assert!(matches!(
        ValidatedSubject::new("events\ndata"),
        Err(SubjectError::InvalidFormat)
    ));
}

#[test]
fn test_invalid_subjects_with_carriage_return() {
    assert!(matches!(
        ValidatedSubject::new("events\rdata"),
        Err(SubjectError::InvalidFormat)
    ));
}

#[test]
fn test_invalid_subjects_starts_with_dot() {
    assert!(matches!(
        ValidatedSubject::new(".events"),
        Err(SubjectError::InvalidFormat)
    ));
}

#[test]
fn test_invalid_subjects_ends_with_dot() {
    assert!(matches!(
        ValidatedSubject::new("events."),
        Err(SubjectError::InvalidFormat)
    ));
}

#[test]
fn test_invalid_subjects_with_consecutive_dots() {
    assert!(matches!(
        ValidatedSubject::new("events..data"),
        Err(SubjectError::InvalidFormat)
    ));
    assert!(matches!(
        ValidatedSubject::new("a..b..c"),
        Err(SubjectError::InvalidFormat)
    ));
}

#[test]
fn test_invalid_empty_subject() {
    assert!(matches!(
        ValidatedSubject::new(""),
        Err(SubjectError::InvalidFormat)
    ));
}

#[test]
fn test_validated_subject_to_subject_trait() {
    let validated = ValidatedSubject::new("test.subject").unwrap();

    // Should convert to Subject without validation overhead
    let subject_result = validated.clone().to_subject_validated();
    assert!(subject_result.is_ok());
    assert_eq!(subject_result.unwrap().as_str(), "test.subject");

    // Should also work with regular to_subject
    let subject = validated.to_subject();
    assert_eq!(subject.as_str(), "test.subject");
}

#[test]
fn test_validated_subject_clone() {
    let validated = ValidatedSubject::new("test.subject").unwrap();
    let cloned = validated.clone();

    assert_eq!(validated.as_str(), cloned.as_str());
}

#[test]
fn test_validated_subject_display() {
    let validated = ValidatedSubject::new("test.subject").unwrap();
    assert_eq!(format!("{}", validated), "test.subject");
}
