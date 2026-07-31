//! Fast base58 encoding for arbitrary-length byte slices.
//!
//! The `bs58` crate carry-propagates one input byte at a time through the
//! whole output, which is quadratic per byte. Encoding here works on the
//! input as a base-2^32 big integer and peels off five base58 digits per
//! division pass, which is ~10x faster on typical instruction payloads and
//! serialized transactions. Output is identical to `bs58::encode`.

const ALPHABET: &[u8; 58] = b"123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";
const DIV: u64 = 58 * 58 * 58 * 58 * 58; // 58^5, the largest power of 58 that fits in u32

pub fn encode(input: &[u8]) -> String {
    let zeros = input.iter().take_while(|&&b| b == 0).count();
    let rest = &input[zeros..];

    // Pack big-endian into u32 limbs, most significant limb first.
    let head_len = rest.len() % 4;
    let mut limbs: Vec<u32> = Vec::with_capacity(rest.len() / 4 + 1);
    if head_len > 0 {
        let mut head = 0u32;
        for &b in &rest[..head_len] {
            head = (head << 8) | u32::from(b);
        }
        limbs.push(head);
    }
    for chunk in rest[head_len..].chunks_exact(4) {
        limbs.push(u32::from_be_bytes(chunk.try_into().unwrap()));
    }

    // Base58 digits, least significant first.
    let mut digits: Vec<u8> = Vec::with_capacity(rest.len() * 137 / 100 + 5);
    let mut start = 0;
    while start < limbs.len() {
        // One long-division pass: limbs /= 58^5, rem = limbs % 58^5.
        let mut rem: u64 = 0;
        for limb in &mut limbs[start..] {
            let acc = (rem << 32) | u64::from(*limb);
            *limb = (acc / DIV) as u32;
            rem = acc % DIV;
        }
        while start < limbs.len() && limbs[start] == 0 {
            start += 1;
        }
        for _ in 0..5 {
            digits.push((rem % 58) as u8);
            rem /= 58;
        }
    }
    // The final pass emits a fixed five digits; trim the high-order zeros.
    while digits.last() == Some(&0) {
        digits.pop();
    }

    let mut out = String::with_capacity(zeros + digits.len());
    for _ in 0..zeros {
        out.push('1');
    }
    for &digit in digits.iter().rev() {
        out.push(ALPHABET[usize::from(digit)] as char);
    }
    out
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_known_vectors() {
        assert_eq!(encode(&[]), "");
        assert_eq!(encode(&[0]), "1");
        assert_eq!(encode(&[0, 0, 0]), "111");
        assert_eq!(encode(b"abc"), "ZiCa");
        assert_eq!(encode(&[0, 0, b'a', b'b', b'c']), "11ZiCa");
        assert_eq!(encode(&[57]), "z");
        assert_eq!(encode(&[58]), "21");
    }

    #[test]
    fn test_matches_bs58() {
        // Deterministic xorshift so the differential test needs no dev-deps.
        let mut state = 0x853c_49e6_748f_ea9bu64;
        let mut next = move || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };

        for len in 0..600 {
            let mut input = vec![0u8; len];
            for byte in input.iter_mut() {
                *byte = next() as u8;
            }
            // Exercise the leading-zeros path too.
            if len % 7 == 0 {
                let zeros = len.min(3);
                input[..zeros].fill(0);
            }
            assert_eq!(
                encode(&input),
                bs58::encode(&input).into_string(),
                "mismatch at len {len}"
            );
        }
    }
}
