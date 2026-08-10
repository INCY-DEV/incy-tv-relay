// Валидация входных данных.
//
// Релей не умеет и не должен проверять содержимое шифротекста — он проверяет
// только форму: длину, алфавит, размер. Всё остальное для него непрозрачно.

// Алфавит кода: без I, O, 0, 1 — их путают при чтении с экрана и диктовке.
export const CODE_ALPHABET = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
export const CODE_LENGTH = 8;

const CODE_RE = new RegExp(`^[${CODE_ALPHABET}]{${CODE_LENGTH}}$`);

export function isValidCode(code) {
  return typeof code === 'string' && CODE_RE.test(code);
}

// base64url без паддинга: A-Z a-z 0-9 - _
const B64URL_RE = /^[A-Za-z0-9_-]+$/;

// Точная длина в байтах после декодирования — чтобы не принять
// укороченную или раздутую точку кривой.
export function decodeB64url(value, expectedBytes) {
  if (typeof value !== 'string' || !value.length) return null;
  if (!B64URL_RE.test(value)) return null;
  // Длина base64 без паддинга однозначно определяется длиной данных;
  // проверяем до декодирования, чтобы не тратить работу на мусор.
  if (expectedBytes != null) {
    const expectedChars = Math.ceil((expectedBytes * 4) / 3);
    if (value.length !== expectedChars) return null;
  }
  let buf;
  try {
    buf = Buffer.from(value, 'base64url');
  } catch {
    return null;
  }
  if (expectedBytes != null && buf.length !== expectedBytes) return null;
  return buf;
}

export const SIZES = {
  sid: 16, // session id от TV
  point: 32, // Ristretto255-точка (Ya / Yb)
  nonce: 12, // ChaCha20-Poly1305
};

// Шифротекст: полезная нагрузка + 16 байт тега Poly1305.
// Верхняя граница — из спецификации (64 KiB).
export const MAX_CT_BYTES = 64 * 1024;
export const MIN_CT_BYTES = 17; // хотя бы 1 байт данных + тег

export function checkCiphertext(value) {
  if (typeof value !== 'string' || !B64URL_RE.test(value)) return null;
  // Отсекаем по длине строки до декодирования: base64 раздувает данные
  // на треть, поэтому декодировать 100 KiB мусора незачем.
  if (value.length > Math.ceil((MAX_CT_BYTES * 4) / 3) + 4) return null;
  let buf;
  try {
    buf = Buffer.from(value, 'base64url');
  } catch {
    return null;
  }
  if (buf.length < MIN_CT_BYTES || buf.length > MAX_CT_BYTES) return null;
  return buf;
}
