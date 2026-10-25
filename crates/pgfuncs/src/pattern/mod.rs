//! LIKE / ILIKE / SIMILAR TO / POSIX regular expressions.

mod like;
mod re;
mod similar;

pub use like::{LikePattern, like_bytes, like_escape_text};
pub use re::{Regex, regex_substring, similar_substring};
pub use similar::{similar_to_regex, similar_to_regex_text};
