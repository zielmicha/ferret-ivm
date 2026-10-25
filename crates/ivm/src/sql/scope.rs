//! Name resolution scopes.

use crate::error::{codes, Result, SqlError};
use crate::types::ColumnType;

#[derive(Clone, Debug)]
pub struct ScopeItem {
    /// Table alias (or name) qualifying this column.
    pub table: Option<String>,
    pub name: String,
    pub ty: ColumnType,
    /// Hidden from `*` expansion and unqualified lookup (USING duplicates,
    /// sort helper columns).
    pub hidden: bool,
}

#[derive(Clone, Debug, Default)]
pub struct Scope {
    pub items: Vec<ScopeItem>,
}

impl Scope {
    pub fn new(items: Vec<ScopeItem>) -> Scope {
        Scope { items }
    }

    pub fn from_table(table: &str, names: &[String], types: &[ColumnType]) -> Scope {
        Scope {
            items: names
                .iter()
                .zip(types)
                .map(|(n, t)| ScopeItem { table: Some(table.to_string()), name: n.clone(), ty: t.clone(), hidden: false })
                .collect(),
        }
    }

    pub fn len(&self) -> usize {
        self.items.len()
    }

    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    pub fn concat(&self, other: &Scope) -> Scope {
        let mut items = self.items.clone();
        items.extend(other.items.iter().cloned());
        Scope { items }
    }

    pub fn types(&self) -> Vec<ColumnType> {
        self.items.iter().map(|i| i.ty.clone()).collect()
    }

    /// Resolve a (possibly qualified) column. Ok(None) if not found here.
    pub fn resolve(&self, table: Option<&str>, name: &str) -> Result<Option<usize>> {
        let mut found: Option<usize> = None;
        for (i, it) in self.items.iter().enumerate() {
            let matches = match table {
                Some(t) => it.table.as_deref() == Some(t) && it.name == name,
                None => !it.hidden && it.name == name,
            };
            if matches {
                if found.is_some() {
                    return Err(SqlError::new(
                        codes::AMBIGUOUS_COLUMN,
                        format!("column reference \"{name}\" is ambiguous"),
                    ));
                }
                found = Some(i);
            }
        }
        Ok(found)
    }

    pub fn has_table(&self, table: &str) -> bool {
        self.items.iter().any(|i| i.table.as_deref() == Some(table))
    }

    /// Columns for `*` (or `table.*`).
    pub fn star(&self, table: Option<&str>) -> Result<Vec<usize>> {
        match table {
            None => Ok((0..self.items.len()).filter(|i| !self.items[*i].hidden).collect()),
            Some(t) => {
                if !self.has_table(t) {
                    return Err(SqlError::new(
                        codes::UNDEFINED_TABLE,
                        format!("missing FROM-clause entry for table \"{t}\""),
                    ));
                }
                Ok((0..self.items.len())
                    .filter(|i| self.items[*i].table.as_deref() == Some(t) && !self.items[*i].name.is_empty())
                    .collect())
            }
        }
    }
}
