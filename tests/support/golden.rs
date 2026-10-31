//! Shared PostgreSQL JSON-lines result, error and panic checking.
#![allow(dead_code)]

use serde_json::Value;

pub trait GoldenError {
    fn fields(&self) -> (&str, &str, Option<&str>);
}

pub struct Golden<E> {
    error: std::marker::PhantomData<E>,
    pub name: String,
    pub records: Vec<Value>,
    pub failures: Vec<String>,
    pub checked: usize,
}

impl<E: GoldenError> Golden<E> {
    pub fn load(file: &str) -> Golden<E> {
        let path = format!("{}/tests/data/{}", env!("CARGO_MANIFEST_DIR"), file);
        Golden::load_path(&path)
    }

    pub fn load_path(path: &str) -> Golden<E> {
        let file = path.rsplit('/').next().unwrap_or(path);
        let text = std::fs::read_to_string(path).unwrap_or_else(|e| panic!("{path}: {e}"));
        let records = text
            .lines()
            .filter(|l| !l.trim().is_empty())
            .map(|l| serde_json::from_str(l).unwrap())
            .collect();
        Golden { name: file.to_string(), records, failures: Vec::new(), checked: 0, error: std::marker::PhantomData }
    }

    /// Runs `f(fn_name, args)` for every record and compares with the
    /// expected result (`Ok(None)` = SQL NULL).
    pub fn run(&mut self, mut f: impl FnMut(&str, &[Value]) -> Result<Option<String>, E>) {
        self.run_records(|rec| f(rec["fn"].as_str().unwrap(), rec["args"].as_array().unwrap()));
    }

    /// As `run`, with access to per-case settings such as the session timezone.
    pub fn run_records(&mut self, mut f: impl FnMut(&Value) -> Result<Option<String>, E>) {
        let records = std::mem::take(&mut self.records);
        for rec in &records {
            self.run_record(rec, &mut f);
        }
        self.records = records;
    }

    /// Run large external corpora without retaining every parsed record.
    pub fn run_path(path: &str, mut f: impl FnMut(&str, &[Value]) -> Result<Option<String>, E>) {
        let text = std::fs::read_to_string(path).unwrap_or_else(|e| panic!("{path}: {e}"));
        let mut g = Golden { name: path.into(), records: Vec::new(), failures: Vec::new(), checked: 0, error: std::marker::PhantomData };
        for line in text.lines().filter(|l| !l.trim().is_empty()) {
            let rec = serde_json::from_str(line).unwrap();
            g.run_record(&rec, &mut |r| f(r["fn"].as_str().unwrap(), r["args"].as_array().unwrap()));
        }
        g.finish();
    }

    fn run_record(&mut self, rec: &Value, f: &mut impl FnMut(&Value) -> Result<Option<String>, E>) {
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| f(rec))) {
            Ok(got) => self.check(rec, got),
            Err(p) => {
                let msg = p.downcast_ref::<String>().map(String::as_str)
                    .or_else(|| p.downcast_ref::<&str>().copied()).unwrap_or_default();
                self.checked += 1;
                self.failures.push(format!("{rec}\n    PANIC: {msg}"));
            }
        }
    }

    pub fn check(&mut self, rec: &Value, got: Result<Option<String>, E>) {
        self.checked += 1;
        let mismatch = match got {
            Err(e) => {
                let (sqlstate, message, detail) = e.fields();
                let expected = &rec["error"];
                (expected["sqlstate"].as_str() != Some(sqlstate)
                    || expected["message"].as_str() != Some(message)
                    || expected["detail"].as_str() != detail)
                    .then(|| format!("error {sqlstate} {message:?} detail={detail:?}"))
            }
            Ok(v) => (rec.get("error").is_some() || v.as_deref() != rec["result"].as_str())
                .then(|| format!("OK: {v:?}")),
        };
        if let Some(shown) = mismatch {
            self.failures.push(format!("{rec}\n    got {shown}"));
        }
    }

    pub fn finish(&self) {
        if !self.failures.is_empty() {
            let n = self.failures.len();
            let shown: Vec<&String> = self.failures.iter().take(60).collect();
            panic!(
                "{}: {} of {} cases failed:\n{}",
                self.name,
                n,
                self.checked,
                shown.iter().map(|s| s.as_str()).collect::<Vec<_>>().join("\n")
            );
        }
        eprintln!("{}: {} cases OK", self.name, self.checked);
    }
}
