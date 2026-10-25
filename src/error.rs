//! [pg] What a deploy failed on, and Postgres's errors in full.
//!
//! A deploy runs statements Postgres may refuse. The `_pg` functions -
//! Table::deploy_pg and deploy_fk_pg, Schema::deploy_all_tables_pg and
//! deploy_all_fk_pg - return that refusal as it came: the native
//! tokio_postgres::Error, with the statement, its file and its step, so a caller
//! (sge) says it its own way, in one place. The String functions over them say it
//! schema_guard's way, through pg_error: tokio_postgres's own Display prints "db
//! error" for every error of the server's, and leaves out what caused one of its own.

use std::fmt::Write;

/// The part of a deploy a statement was for
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeployStep {
    /// CREATE or ALTER TABLE, its columns, triggers and keys
    Table,
    Comments,
    Indexes,
    Grants,
    /// the rows of a table's `data`
    Data,
    ForeignKeys,
}

impl DeployStep {
    /// the message (locales/table.yml) saying a statement of this step failed
    fn key(&self) -> &'static str {
        match self {
            DeployStep::Table => "table.exec",
            DeployStep::Comments => "table.exec_comments",
            DeployStep::Indexes => "table.exec_indexes",
            DeployStep::Grants => "table.exec_grants",
            DeployStep::Data => "table.exec_data",
            DeployStep::ForeignKeys => "table.fk_execute",
        }
    }
}

/// What a deploy failed on
#[derive(Debug)]
pub enum DeployError {
    /// a statement Postgres refused: its native error, the statement, the file it came
    /// from (empty for none) and the step it was for
    Pg { step: DeployStep, sql: String, file: String, err: tokio_postgres::Error },
    /// one of schema_guard's own, a message in the messages' language (i18n.rs)
    Message(String),
}

impl From<String> for DeployError {
    fn from(message: String) -> Self {
        DeployError::Message(message)
    }
}

impl std::fmt::Display for DeployError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DeployError::Pg { step, sql, file, err } => {
                let source = if file.is_empty() { String::new() } else { tr!("table.source", file = file) };
                f.write_str(&tr!(step.key(), sql = sql, source = source, err = pg_error_in(err, sql)))
            }
            DeployError::Message(m) => f.write_str(m),
        }
    }
}

impl std::error::Error for DeployError {
    /// the native tokio_postgres::Error of a statement Postgres refused, for a caller
    /// walking the chain (anyhow, eyre)
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            DeployError::Pg { err, .. } => Some(err),
            DeployError::Message(_) => None,
        }
    }
}

/// A Postgres error in full, which tokio_postgres's Display is not: one of the server's
/// with its severity, SQLSTATE and message - in the server's language, its lc_messages -
/// its DETAIL, HINT and WHERE, and the character it points at; one of the client's with
/// what caused it, as "error connecting to server: Connection refused (os error 111)".
/// pg_error_in, given the statement, says the line of it that character is on.
pub fn pg_error(e: &tokio_postgres::Error) -> String {
    pg_error_in(e, "")
}

/// [pg_error], with the line of `sql` - the statement the error is of - the server points
/// at, as psql says it: `LINE 2: ...` and a `^` under the character. Without the
/// statement (empty) the position is said as a number.
pub fn pg_error_in(e: &tokio_postgres::Error, sql: &str) -> String {
    match e.as_db_error() {
        Some(db) => {
            let mut m = format!("{} {}: {}", db.severity(), db.code().code(), db.message());
            match db.position() {
                Some(tokio_postgres::error::ErrorPosition::Original(p)) => {
                    let _ = write!(m, "\n{}", position(sql, *p));
                }
                // in a statement the server ran for one of ours - a function's body
                Some(tokio_postgres::error::ErrorPosition::Internal { position: p, query }) => {
                    let _ = write!(m, "\n{}", position(query, *p));
                }
                None => {}
            }
            if let Some(detail) = db.detail() {
                let _ = write!(m, "\n{}", tr!("pg.detail", text = detail));
            }
            if let Some(hint) = db.hint() {
                let _ = write!(m, "\n{}", tr!("pg.hint", text = hint));
            }
            if let Some(context) = db.where_() {
                let _ = write!(m, "\n{}", tr!("pg.where", text = context));
            }
            m
        }
        None => {
            let mut m = e.to_string();
            let mut cause = std::error::Error::source(e);
            while let Some(c) = cause {
                let _ = write!(m, ": {}", c);
                cause = c.source();
            }
            m
        }
    }
}

/// The server's error position `p` - its 1-based character in `sql` - as the line it is
/// on with a `^` under it; as the number, when `sql` has no such character
fn position(sql: &str, p: u32) -> String {
    match line_at(sql, p) {
        Some((line, text, col)) => {
            let head = tr!("pg.line", line = line, text = text);
            // the ^ under the character: past the message's own words, then the column
            let pad = head.chars().count() - text.chars().count() + col - 1;
            format!("{}\n{}^", head, " ".repeat(pad))
        }
        None => tr!("pg.position", position = p),
    }
}

/// the 1-based line of character `p` of `sql`, that line's text, and the character's
/// 1-based column in it
fn line_at(sql: &str, p: u32) -> Option<(usize, &str, usize)> {
    let (at, _) = sql.char_indices().nth((p as usize).checked_sub(1)?)?;
    let start = sql[..at].rfind('\n').map_or(0, |i| i + 1);
    let end = sql[at..].find('\n').map_or(sql.len(), |i| at + i);
    let line = sql[..at].matches('\n').count() + 1;
    let text = sql[start..end].trim_end_matches('\r');
    Some((line, text, sql[start..at].chars().count() + 1))
}

/// [i18n] Ask the server for its messages - errors, notices - in `lang` (`en`, `ru`,
/// ...), through lc_messages: LANG's own locale when it is of that language, else the
/// language's usual one (ru_RU.UTF-8). Only when the role may set it - a superuser, or
/// one granted SET on it (PostgreSQL 15 and later) - and the server has the locale;
/// otherwise its messages stay in its own language, and nothing fails: asking is no
/// reason for a run to stop. Call it before a transaction is begun: a SET the server
/// refuses - a locale it has not - aborts the transaction it is in.
pub async fn set_lc_messages(client: &tokio_postgres::Client, lang: &str) {
    let locale = locale_of(lang, std::env::var("LANG").ok().as_deref());
    let may = match client.query_one("select current_setting('is_superuser') = 'on', \
        current_setting('server_version_num')::int >= 150000", &[]).await {
        Ok(r) if r.get::<_, bool>(0) => true,
        // has_parameter_privilege came with PostgreSQL 15: asked only of one that has it
        Ok(r) if r.get::<_, bool>(1) => client
            .query_one("select has_parameter_privilege('lc_messages', 'SET')", &[]).await
            .map(|r| r.get::<_, bool>(0)).unwrap_or(false),
        _ => false,
    };
    if may {
        let _ = client.batch_execute(&format!("SET lc_messages TO '{}'", locale)).await;
    }
}

/// the locale lc_messages is set to for `lang`: LANG's when it is of that language
/// (ru_RU.UTF-8 for ru), else the language's usual one. Letters, digits, `_`, `.`,
/// `-` and `@` only: it goes into a SET statement
fn locale_of(lang: &str, env: Option<&str>) -> String {
    let lang = lang.trim().to_lowercase();
    let safe = |l: &str| !l.is_empty() && l.chars().all(|c| c.is_ascii_alphanumeric() || "_.-@".contains(c));
    if let Some(env) = env.map(str::trim).filter(|e| safe(e) && e.to_lowercase().starts_with(&lang) && e.contains('_')) {
        return env.to_string();
    }
    match lang.as_str() {
        "ru" => "ru_RU.UTF-8",
        "fr" => "fr_FR.UTF-8",
        "de" => "de_DE.UTF-8",
        "zh" => "zh_CN.UTF-8",
        "ja" => "ja_JP.UTF-8",
        _ => "en_US.UTF-8",
    }.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// [i18n] LANG's own locale when it is of the language, the usual one otherwise -
    /// and never anything that could break out of the SET statement
    #[test]
    fn the_locale_asked_for_is_the_users() {
        assert_eq!(locale_of("ru", Some("ru_RU.UTF-8")), "ru_RU.UTF-8");
        assert_eq!(locale_of("ru", Some("ru_UA.UTF-8")), "ru_UA.UTF-8");
        assert_eq!(locale_of("ru", Some("en_US.UTF-8")), "ru_RU.UTF-8", "set_lang over LANG");
        assert_eq!(locale_of("ru", None), "ru_RU.UTF-8");
        assert_eq!(locale_of("zh", Some("C.UTF-8")), "zh_CN.UTF-8");
        assert_eq!(locale_of("en", Some("en_GB.UTF-8")), "en_GB.UTF-8");
        assert_eq!(locale_of("xx", None), "en_US.UTF-8");
        assert_eq!(locale_of("ru", Some("ru_RU'; drop table t; --")), "ru_RU.UTF-8");
    }

    /// [pg] a client error says what caused it - tokio_postgres's Display alone does not
    #[test]
    fn a_client_error_says_its_cause() {
        let e = "host=/nonexistent port=1 user=x".parse::<tokio_postgres::Config>().unwrap();
        let rt = tokio::runtime::Runtime::new().unwrap();
        let err = rt.block_on(e.connect(tokio_postgres::NoTls)).err().unwrap();
        let full = pg_error(&err);
        assert!(full.starts_with(&err.to_string()) && full.len() > err.to_string().len(), "{}", full);
    }

    /// [pg] a deploy's own message, as it is, and with nothing under it
    #[test]
    fn a_message_is_said_as_it_is() {
        let e = DeployError::from("SG-E030 x".to_string());
        assert_eq!(e.to_string(), "SG-E030 x");
        assert!(std::error::Error::source(&e).is_none());
    }

    /// [pg] a refused statement's error carries Postgres's as its source, for a chain
    #[test]
    fn a_refused_statement_has_its_error_as_source() {
        let c = "host=/nonexistent port=1 user=x".parse::<tokio_postgres::Config>().unwrap();
        let rt = tokio::runtime::Runtime::new().unwrap();
        let err = rt.block_on(c.connect(tokio_postgres::NoTls)).err().unwrap();
        let said = err.to_string();
        let e = DeployError::Pg { step: DeployStep::Indexes, sql: "create index i on t (a)".into(), file: "S1__a.yaml".into(), err };
        assert_eq!(std::error::Error::source(&e).map(|s| s.to_string()), Some(said));
        let m = e.to_string();
        assert!(m.starts_with("SG-E") && m.contains("create index i on t (a)") && m.contains("S1__a.yaml"), "{}", m);
    }

    /// [pg] the character the server points at: its line, and the column in it
    #[test]
    fn a_position_is_found_on_its_line() {
        let sql = "create table t (\n  a nosuchtype,\r\n  b int\n)";
        assert_eq!(line_at(sql, 1), Some((1, "create table t (", 1)));
        assert_eq!(line_at(sql, 22), Some((2, "  a nosuchtype,", 5)));
        assert_eq!(line_at(sql, 37), Some((3, "  b int", 3)));
        assert_eq!(line_at("ты нет", 4), Some((1, "ты нет", 4)), "characters, not bytes");
        assert_eq!(line_at(sql, 0), None);
        assert_eq!(line_at(sql, 1000), None);
        assert_eq!(line_at("", 1), None);
    }

    /// [pg] the ^ stands under the character, whatever the language's words before it
    #[test]
    fn the_caret_is_under_the_character() {
        let out = position("create table t (\n  a nosuchtype\n)", 22);
        let lines: Vec<&str> = out.split('\n').collect();
        assert_eq!(lines.len(), 2, "{}", out);
        assert!(lines[0].ends_with("  a nosuchtype") && lines[1].trim() == "^", "{}", out);
        assert_eq!(lines[0].chars().nth(lines[1].chars().count() - 1), Some('n'), "{}", out);
        assert!(position("select 1", 99).ends_with(" 99") || position("select 1", 99).ends_with("：99"));
    }
}
