// SheetORM — UUID generation utility for Google Apps Script
// Uses crypto-safe random when available, falls back to Math.random

export function generateUUID(): string {
  // GAS V8 runtime supports Utilities.getUuid() but we provide a fallback
  // for testing environments
  if (
    typeof Utilities !== 'undefined' &&
    typeof Utilities.getUuid === 'function'
  ) {
    return Utilities.getUuid();
  }
  // RFC 4122 v4 UUID fallback
  return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, (c) => {
    const r = (Math.random() * 16) | 0;
    const v = c === 'x' ? r : (r & 0x3) | 0x8;
    return v.toString(16);
  });
}
