//! Client-side password authentication: MD5 and SCRAM-SHA-256.

use base64::Engine;
use base64::engine::general_purpose::STANDARD as B64;
use hmac::{Hmac, KeyInit, Mac};
use md5::Md5;
use sha2::{Digest, Sha256};

use crate::{Error, Result};

type HmacSha256 = Hmac<Sha256>;

fn hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut s = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        s.push(HEX[(b >> 4) as usize] as char);
        s.push(HEX[(b & 15) as usize] as char);
    }
    s
}

/// `"md5" || md5hex(md5hex(password || user) || salt)`
pub(crate) fn md5_password(user: &str, password: &str, salt: [u8; 4]) -> String {
    let mut h = Md5::new();
    h.update(password.as_bytes());
    h.update(user.as_bytes());
    let inner = hex(&h.finalize());
    let mut h = Md5::new();
    h.update(inner.as_bytes());
    h.update(salt);
    format!("md5{}", hex(&h.finalize()))
}

pub(crate) fn random_bytes(buf: &mut [u8]) -> Result<()> {
    getrandom::fill(buf).map_err(|e| Error::auth(format!("could not generate random nonce: {e}")))
}

fn hmac(key: &[u8], parts: &[&[u8]]) -> [u8; 32] {
    let mut m = HmacSha256::new_from_slice(key).expect("HMAC accepts any key length");
    for p in parts {
        m.update(p);
    }
    m.finalize().into_bytes().into()
}

/// PBKDF2-HMAC-SHA256 with a single output block (SCRAM's `Hi()`).
fn hi(password: &[u8], salt: &[u8], iterations: u32) -> [u8; 32] {
    let base = HmacSha256::new_from_slice(password).expect("HMAC accepts any key length");
    let mut m = base.clone();
    m.update(salt);
    m.update(&1u32.to_be_bytes());
    let mut u: [u8; 32] = m.finalize().into_bytes().into();
    let mut out = u;
    for _ in 1..iterations {
        let mut m = base.clone();
        m.update(&u);
        u = m.finalize().into_bytes().into();
        for (o, x) in out.iter_mut().zip(u.iter()) {
            *o ^= x;
        }
    }
    out
}

/// SCRAM-SHA-256 client state machine (RFC 5802 / 7677), without channel
/// binding (gs2 header `n,,`).
pub(crate) struct ScramClient {
    password: Vec<u8>,
    client_nonce: String,
    client_first_bare: String,
    expected_server_signature: Option<[u8; 32]>,
}

pub(crate) const SCRAM_SHA_256: &str = "SCRAM-SHA-256";

impl ScramClient {
    pub(crate) fn new(password: &str) -> Result<ScramClient> {
        let mut raw = [0u8; 18];
        random_bytes(&mut raw)?;
        Ok(Self::with_nonce(password, B64.encode(raw)))
    }

    fn with_nonce(password: &str, client_nonce: String) -> ScramClient {
        // PostgreSQL ignores the SCRAM user name (it uses the startup user).
        let client_first_bare = format!("n=,r={client_nonce}");
        ScramClient {
            // NOTE: SASLprep normalization is not applied; it is the identity
            // for ASCII passwords.
            password: password.as_bytes().to_vec(),
            client_nonce,
            client_first_bare,
            expected_server_signature: None,
        }
    }

    /// The client-first-message (payload of SASLInitialResponse).
    pub(crate) fn client_first(&self) -> Vec<u8> {
        format!("n,,{}", self.client_first_bare).into_bytes()
    }

    /// Processes the server-first-message, returning the client-final-message.
    pub(crate) fn server_first(&mut self, data: &[u8]) -> Result<Vec<u8>> {
        let server_first = std::str::from_utf8(data)
            .map_err(|_| Error::auth("SCRAM: server-first-message is not valid UTF-8"))?;
        let mut nonce = None;
        let mut salt = None;
        let mut iterations = None;
        for attr in server_first.split(',') {
            let (k, v) = attr
                .split_once('=')
                .ok_or_else(|| Error::auth("SCRAM: malformed server-first-message"))?;
            match k {
                "r" => nonce = Some(v),
                "s" => salt = Some(v),
                "i" => iterations = Some(v),
                "m" => return Err(Error::auth("SCRAM: unsupported mandatory extension")),
                _ => {}
            }
        }
        let (Some(nonce), Some(salt), Some(iterations)) = (nonce, salt, iterations) else {
            return Err(Error::auth("SCRAM: incomplete server-first-message"));
        };
        if !nonce.starts_with(&self.client_nonce) || nonce.len() <= self.client_nonce.len() {
            return Err(Error::auth(
                "SCRAM: server nonce does not extend client nonce",
            ));
        }
        let salt = B64
            .decode(salt)
            .map_err(|_| Error::auth("SCRAM: invalid salt encoding"))?;
        let iterations: u32 = iterations
            .parse()
            .ok()
            .filter(|&i| i > 0)
            .ok_or_else(|| Error::auth("SCRAM: invalid iteration count"))?;

        let salted = hi(&self.password, &salt, iterations);
        let client_key = hmac(&salted, &[b"Client Key"]);
        let stored_key: [u8; 32] = Sha256::digest(client_key).into();
        // "biws" is base64("n,,")
        let final_without_proof = format!("c=biws,r={nonce}");
        let auth_message = format!(
            "{},{},{}",
            self.client_first_bare, server_first, final_without_proof
        );
        let client_signature = hmac(&stored_key, &[auth_message.as_bytes()]);
        let mut proof = client_key;
        for (p, s) in proof.iter_mut().zip(client_signature.iter()) {
            *p ^= s;
        }
        let server_key = hmac(&salted, &[b"Server Key"]);
        self.expected_server_signature = Some(hmac(&server_key, &[auth_message.as_bytes()]));
        Ok(format!("{final_without_proof},p={}", B64.encode(proof)).into_bytes())
    }

    /// Verifies the server-final-message.
    pub(crate) fn server_final(&self, data: &[u8]) -> Result<()> {
        let s = std::str::from_utf8(data)
            .map_err(|_| Error::auth("SCRAM: server-final-message is not valid UTF-8"))?;
        if let Some(e) = s.strip_prefix("e=") {
            return Err(Error::auth(format!("SCRAM: server error: {e}")));
        }
        let v = s
            .split(',')
            .find_map(|a| a.strip_prefix("v="))
            .ok_or_else(|| Error::auth("SCRAM: malformed server-final-message"))?;
        let sig = B64
            .decode(v)
            .map_err(|_| Error::auth("SCRAM: invalid server signature"))?;
        match &self.expected_server_signature {
            Some(expected) if sig.as_slice() == expected => Ok(()),
            _ => Err(Error::auth("SCRAM: server signature mismatch")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn md5_known_value() {
        // Python: 'md5' + md5(md5(b'secretalice').hexdigest().encode() + b'abcd').hexdigest()
        assert_eq!(
            md5_password("alice", "secret", *b"abcd"),
            "md5c90bcafc0a2b54511742383e34d5dc9d"
        );
    }

    /// RFC 7677 section 3 test vector.
    #[test]
    fn scram_rfc7677_vector() {
        let mut c = ScramClient::with_nonce("pencil", "rOprNGfwEbeRWgbNEkqO".into());
        // The RFC uses n=user; PostgreSQL sends an empty user name, so build
        // the RFC variant by hand.
        c.client_first_bare = "n=user,r=rOprNGfwEbeRWgbNEkqO".into();
        let server_first =
            b"r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096";
        let fin = c.server_first(server_first).unwrap();
        assert_eq!(
            String::from_utf8(fin).unwrap(),
            "c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,p=dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ="
        );
        c.server_final(b"v=6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4=")
            .unwrap();
        assert!(
            c.server_final(b"v=AAAATRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4=")
                .is_err()
        );
        assert!(c.server_final(b"e=invalid-proof").is_err());
    }

    #[test]
    fn scram_rejects_bad_nonce() {
        let mut c = ScramClient::with_nonce("pw", "abc".into());
        assert!(c.server_first(b"r=xyzdef,s=AAAA,i=4096").is_err());
        assert!(c.server_first(b"r=abc,s=AAAA,i=4096").is_err());
        assert!(c.server_first(b"r=abcdef,s=AAAA,i=0").is_err());
    }
}
